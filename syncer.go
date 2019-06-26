package main

import (
	"context"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/sys/unix"
)

type syncer struct {
	bucket              string
	prefix              string
	dst                 string
	linkObjectKeyRegexp *regexp.Regexp
	s3Api               s3iface.S3API
}

func newSyncer(
	region, bucket, prefix, dst string,
	linkObjectKeyRegexp *regexp.Regexp,
	awsClientFactory awsClientFactory,
) *syncer {
	return &syncer{
		bucket:              bucket,
		prefix:              prefix,
		dst:                 dst,
		linkObjectKeyRegexp: linkObjectKeyRegexp,
		s3Api:               awsClientFactory.newS3(region),
	}
}

func (s *syncer) sync(ctx context.Context) (bool, error) {
	path := s.dst
	if !strings.HasSuffix(path, string(filepath.Separator)) {
		path += string(filepath.Separator)
	}
	destination := destination{path: path}

	prefix, err := s.resolveLinks(ctx, s.prefix)
	if err != nil {
		return false, err
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	source := source{
		bucket:              s.bucket,
		prefix:              prefix,
		linkObjectKeyRegexp: s.linkObjectKeyRegexp,
		s3Api:               s.s3Api,
	}

	files, err := destination.files()
	if err != nil {
		return false, err
	}

	objects, err := source.objects(ctx)
	if err != nil {
		return false, err
	}

	added, removed := s.diff(files, objects)

	if err := s.updateFiles(ctx, added); err != nil {
		return false, err
	}

	if err := s.removeFiles(removed); err != nil {
		return false, err
	}

	return len(added) > 0 || len(removed) > 0, nil
}

func (s *syncer) resolveLinks(ctx context.Context, key string) (string, error) {
	if s.linkObjectKeyRegexp == nil {
		return key, nil
	}

	var k string
	for _, c := range strings.Split(strings.TrimSuffix(key, "/"), "/") {
		if len(k) > 0 {
			k += "/"
		}
		k += c
		if s.linkObjectKeyRegexp.MatchString(k) {
			var err error
			if k, err = s.resolveLink(ctx, k); err != nil {
				return "", err
			}
		}
	}
	return k, nil
}

func (s *syncer) resolveLink(ctx context.Context, key string) (string, error) {
	dst, err := readLinkObject(ctx, s.s3Api, s.bucket, key)
	if err != nil {
		return "", err
	}
	return path.Join(path.Dir(key), dst), nil
}

func readLinkObject(ctx context.Context, s3Api s3iface.S3API, bucket, key string) (string, error) {
	input := s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	output, err := s3Api.GetObjectWithContext(ctx, &input)
	if err != nil {
		return "", err
	}

	body, err := ioutil.ReadAll(output.Body)
	if err != nil {
		return "", err
	}

	return strings.TrimRight(string(body), "\r\n"), nil
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
			if file.link != object.link ||
				file.link == "" && (file.size != object.size || file.modTime.Before(object.modTime)) {
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

func (s *syncer) updateFiles(ctx context.Context, objects []*object) error {
	downloader := s3manager.NewDownloaderWithClient(s.s3Api)
	for _, o := range objects {
		if err := s.updateFile(ctx, o, downloader); err != nil {
			return err
		}
	}
	return nil
}

func (s *syncer) updateFile(ctx context.Context, object *object, downloader *s3manager.Downloader) error {
	dst := filepath.Join(s.dst, object.compareKey)

	if err := os.MkdirAll(filepath.Dir(dst), os.ModePerm); err != nil {
		return err
	}

	fileName := filepath.Join(filepath.Dir(dst), "."+filepath.Base(dst)+strconv.Itoa(int(rand.Int31())))
	if object.link != "" {
		log.Printf("Updating %s with a symbolic link to %s...\n", dst, object.link)

		if err := os.Symlink(object.link, fileName); err != nil {
			return err
		}
	} else {
		log.Printf("Updating %s with s3://%s/%s...\n", dst, s.bucket, object.key)

		file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.ModePerm)
		if err != nil {
			return err
		}
		defer file.Close()

		input := s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(object.key)}
		if _, err := downloader.DownloadWithContext(ctx, file, &input); err != nil {
			return err
		}
	}

	t := unix.NsecToTimeval(object.modTime.UnixNano())
	if err := unix.Lutimes(fileName, []unix.Timeval{t, t}); err != nil {
		return err
	}

	if err := os.Rename(fileName, dst); err != nil {
		return err
	}

	return nil
}

func (s *syncer) removeFiles(files []*file) error {
	for _, f := range files {
		log.Printf("Removing %s...\n", f.path)

		if err := os.Remove(f.path); err != nil {
			return err
		}
	}

	return nil
}

type destination struct {
	path string
}

func (d *destination) files() (*fileIterator, error) {
	if _, err := os.Stat(d.path); os.IsNotExist(err) {
		return &fileIterator{}, nil
	}

	var files []*file
	err := filepath.Walk(d.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		var link string
		if info.Mode()&os.ModeSymlink != 0 {
			if link, err = os.Readlink(path); err != nil {
				return err
			}
		}

		files = append(files, &file{
			compareKey: strings.TrimPrefix(path, d.path),
			link:       link,
			modTime:    info.ModTime(),
			path:       path,
			size:       info.Size(),
		})

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
	link       string
	modTime    time.Time
	path       string
	size       int64
}

type source struct {
	bucket              string
	prefix              string
	linkObjectKeyRegexp *regexp.Regexp
	s3Api               s3iface.S3API
}

func (s *source) objects(ctx context.Context) (*objectIterator, error) {
	var err error
	var objects []*object

	input := s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(s.prefix)}
	e := s.s3Api.ListObjectsV2PagesWithContext(ctx, &input, func(output *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, o := range output.Contents {
			key := aws.StringValue(o.Key)
			var link string
			if s.linkObjectKeyRegexp != nil && s.linkObjectKeyRegexp.MatchString(key) {
				if link, err = readLinkObject(ctx, s.s3Api, s.bucket, key); err != nil {
					return false
				}
			}

			objects = append(objects, &object{
				compareKey: strings.TrimPrefix(key, s.prefix),
				key:        key,
				link:       link,
				modTime:    aws.TimeValue(o.LastModified),
				size:       aws.Int64Value(o.Size),
			})
		}
		return true
	})
	if e != nil {
		return nil, e
	}
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
	link       string
	modTime    time.Time
	size       int64
}
