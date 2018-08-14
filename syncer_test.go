package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type testFile struct {
	content string
	path    string
	modTime time.Time
}

func (o *testFile) size() int64 {
	return int64(len(o.content))
}

type testObject struct {
	content      string
	key          string
	lastModified time.Time
}

func (o *testObject) size() int64 {
	return int64(len(o.content))
}

type s3Api struct {
	s3iface.S3API
	objects []*testObject
}

func (a *s3Api) ListObjectsV2PagesWithContext(ctx aws.Context, input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool, opts ...request.Option) error {
	for i, o := range a.objects {
		output := s3.ListObjectsV2Output{}
		output.Contents = []*s3.Object{
			&s3.Object{
				Key:          aws.String(o.key),
				LastModified: aws.Time(o.lastModified),
				Size:         aws.Int64(o.size()),
			},
		}
		fn(&output, len(a.objects) == i+1)
	}
	return nil
}

func (a *s3Api) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	for _, o := range a.objects {
		if o.key != aws.StringValue(input.Key) {
			continue
		}
		output := s3.GetObjectOutput{}
		output.Body = ioutil.NopCloser(strings.NewReader(o.content))
		output.ContentLength = aws.Int64(o.size())
		return &output, nil
	}
	return nil, fmt.Errorf("object not found. key=%s", aws.StringValue(input.Key))
}

func TestSync(t *testing.T) {
	modTime := time.Now()
	prefix := "prefix"

	file1 := &testFile{content: "a", path: "key1", modTime: modTime}
	file1New := &testFile{content: "a", path: "key1", modTime: modTime.Add(time.Second)}
	file1Old := &testFile{content: "a", path: "key1", modTime: modTime.Add(-time.Second)}
	file1Large := &testFile{content: "aa", path: "key1", modTime: modTime}
	file2 := &testFile{content: "aa", path: "key2", modTime: modTime}
	file3 := &testFile{content: "aaa", path: "key3", modTime: modTime}

	object1 := &testObject{content: "a", key: filepath.Join(prefix, "key1"), lastModified: modTime}
	object2 := &testObject{content: "aa", key: filepath.Join(prefix, "key2"), lastModified: modTime}

	testSync(t, prefix, []*testFile{}, []*testObject{object1, object2}, []*testFile{file1, file2})
	testSync(t, prefix, []*testFile{file1}, []*testObject{object1, object2}, []*testFile{file1, file2})
	testSync(t, prefix, []*testFile{file1New}, []*testObject{object1, object2}, []*testFile{file1New, file2})
	testSync(t, prefix, []*testFile{file1Old}, []*testObject{object1, object2}, []*testFile{file1, file2})
	testSync(t, prefix, []*testFile{file1Large}, []*testObject{object1, object2}, []*testFile{file1, file2})
	testSync(t, prefix, []*testFile{file1, file2}, []*testObject{object1, object2}, []*testFile{file1, file2})
	testSync(t, prefix, []*testFile{file1, file2, file3}, []*testObject{object1, object2}, []*testFile{file1, file2})
	testSync(t, prefix, []*testFile{file1, file3}, []*testObject{object1, object2}, []*testFile{file1, file2})
}

func testSync(t *testing.T, prefix string, files []*testFile, objects []*testObject, expectedFiles []*testFile) {
	dir, err := ioutil.TempDir("", "syncer_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	for _, f := range files {
		path := filepath.Join(dir, f.path)

		if err := ioutil.WriteFile(path, []byte(f.content), os.ModePerm); err != nil {
			t.Fatal(err)
		}

		if err := os.Chtimes(path, f.modTime, f.modTime); err != nil {
			t.Fatal(err)
		}
	}

	api := &s3Api{objects: objects}

	syncer := syncer{
		bucket: "bucket",
		prefix: prefix,
		dst:    dir,
		s3Api:  api,
	}
	changed, err := syncer.sync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if changed != !reflect.DeepEqual(files, expectedFiles) {
		t.Errorf("syncer.sync: got %t, want %t", changed, reflect.DeepEqual(files, expectedFiles))
		return
	}

	files = make([]*testFile, 0, len(objects))
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		content, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		file := testFile{
			content: string(content),
			modTime: info.ModTime(),
			path:    strings.TrimPrefix(strings.TrimPrefix(path, dir), string(filepath.Separator)),
		}

		files = append(files, &file)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != len(expectedFiles) {
		t.Errorf("expected %d files, got %d files", len(objects), len(files))
		return
	}

	for i, f := range files {
		e := expectedFiles[i]
		if f.path != e.path {
			t.Errorf("path: got %q, want %q", f.path, e.path)
			return
		}
		if !f.modTime.Equal(e.modTime) {
			t.Errorf("path=%s, modTime: got %q, want %q", f.path, f.modTime, e.modTime)
			return
		}
		if f.content != e.content {
			t.Errorf("path=%s, content: got %q, want %q", f.path, f.content, e.content)
			return
		}
	}
}
