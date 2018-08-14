package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type syncer struct {
	bucket string
	dst    string
	prefix string
	s3Api  s3iface.S3API
}

func newSyncer(region, bucket, prefix, dst string, awsClientFactory awsClientFactory) *syncer {
	return &syncer{
		bucket: bucket,
		dst:    dst,
		prefix: prefix,
		s3Api:  awsClientFactory.newS3(region),
	}
}

func (s *syncer) sync(ctx context.Context) (bool, error) {
	path := s.dst
	if !strings.HasSuffix(path, string(filepath.Separator)) {
		path += string(filepath.Separator)
	}
	destination := destination{path: path}

	prefix := s.prefix
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	source := source{bucket: s.bucket, prefix: prefix, s3Api: s.s3Api}

	files, err := destination.files()
	if err != nil {
		return false, err
	}

	objects, err := source.objects(ctx)
	if err != nil {
		return false, err
	}

	added, removed := s.diff(files, objects)

	if err := s.download(ctx, added); err != nil {
		return false, err
	}

	if err := s.remove(removed); err != nil {
		return false, err
	}

	return len(added) > 0 || len(removed) > 0, nil
}

func (s *syncer) diff(files *fileIterator, objects *objectIterator) (added []*object, removed []*file) {
	for {
		file := files.peek()
		object := objects.peek()

		if file == nil || object == nil {
			break
		}

		switch strings.Compare(file.compareKey, object.compareKey) {
		case 0:
			if file.size != object.size || file.modTime.Before(object.modTime) {
				added = append(added, object)
			}
			files.next()
			objects.next()
		case -1:
			removed = append(removed, file)
			files.next()
		case 1:
			added = append(added, object)
			objects.next()
		}
	}

	for file := files.next(); file != nil; file = files.next() {
		removed = append(removed, file)
	}

	for object := objects.next(); object != nil; object = objects.next() {
		added = append(added, object)
	}

	return
}

func (s *syncer) download(ctx context.Context, objects []*object) error {
	downloader := s3manager.NewDownloaderWithClient(s.s3Api)
	for _, o := range objects {
		dst := filepath.Join(s.dst, o.compareKey)

		if err := os.MkdirAll(path.Dir(dst), os.ModePerm); err != nil {
			return err
		}

		file, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
		if err != nil {
			return err
		}
		defer file.Close()

		log.Printf("Downloading %s to %s\n", fmt.Sprintf("s3://%s/%s", s.bucket, o.key), dst)

		input := s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(o.key)}
		if _, err := downloader.DownloadWithContext(ctx, file, &input); err != nil {
			return err
		}

		if err := os.Chtimes(file.Name(), o.modTime, o.modTime); err != nil {
			return err
		}
	}
	return nil
}

func (s *syncer) remove(files []*file) error {
	for _, f := range files {
		log.Printf("Removing %s\n", f.path)

		if err := os.Remove(f.path); err != nil {
			return err
		}
	}

	return nil
}

type destination struct {
	path string
}

func (l *destination) files() (*fileIterator, error) {
	if _, err := os.Stat(l.path); os.IsNotExist(err) {
		return &fileIterator{}, nil
	}

	var files []*file
	err := filepath.Walk(l.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		compareKey := strings.TrimPrefix(path, l.path)
		size := info.Size()
		modTime := info.ModTime()
		files = append(files, &file{compareKey: compareKey, modTime: modTime, path: path, size: size})

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &fileIterator{files: files}, nil
}

type fileIterator struct {
	files []*file
	i     int
}

func (i *fileIterator) peek() *file {
	if len(i.files) > i.i {
		return i.files[i.i]
	}
	return nil
}

func (i *fileIterator) next() *file {
	if len(i.files) > i.i {
		i.i++
		return i.files[i.i-1]
	}
	return nil
}

type file struct {
	compareKey string
	modTime    time.Time
	path       string
	size       int64
}

type source struct {
	s3Api  s3iface.S3API
	bucket string
	prefix string
}

func (r *source) objects(ctx context.Context) (*objectIterator, error) {
	var objects []*object
	input := s3.ListObjectsV2Input{Bucket: aws.String(r.bucket), Prefix: aws.String(r.prefix)}
	err := r.s3Api.ListObjectsV2PagesWithContext(ctx, &input, func(output *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, o := range output.Contents {
			key := aws.StringValue(o.Key)
			compareKey := strings.TrimPrefix(key, r.prefix)
			modTime := aws.TimeValue(o.LastModified)
			size := aws.Int64Value(o.Size)
			objects = append(objects, &object{compareKey: compareKey, key: key, modTime: modTime, size: size})
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return &objectIterator{objects: objects}, nil
}

type objectIterator struct {
	objects []*object
	i       int
}

func (i *objectIterator) peek() *object {
	if len(i.objects) > i.i {
		return i.objects[i.i]
	}
	return nil
}

func (i *objectIterator) next() *object {
	if len(i.objects) > i.i {
		i.i++
		return i.objects[i.i-1]
	}
	return nil
}

type object struct {
	compareKey string
	key        string
	modTime    time.Time
	size       int64
}
