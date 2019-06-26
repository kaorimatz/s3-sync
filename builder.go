package main

import (
	"archive/tar"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecr"
	"github.com/aws/aws-sdk-go/service/ecr/ecriface"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

type builder struct {
	auths     map[name.Tag]authenticator
	baseImage v1.Image
	cmd       []string
	paths     []string
}

func newBuilderFromSyncSpecs(tags []string, specs []*syncSpec, awsClientFactory awsClientFactory) (*builder, error) {
	paths := make([]string, 0, len(specs))
	cmd := make([]string, 0, len(specs)*2)
	for _, s := range specs {
		paths = append(paths, s.dst)

		value, err := s.toCSV()
		if err != nil {
			return nil, err
		}
		cmd = append(cmd, []string{"--sync", value}...)
	}
	return newBuilder(tags, paths, cmd, awsClientFactory)
}

func newBuilder(tags, paths, cmd []string, awsClientFactory awsClientFactory) (*builder, error) {
	k := keychain{awsClientFactory: awsClientFactory}
	auths := make(map[name.Tag]authenticator)
	for _, tag := range tags {
		t, err := name.NewTag(tag, name.WeakValidation)
		if err != nil {
			return nil, err
		}

		auth, err := k.resolve(t.Context().Registry)
		if err != nil {
			return nil, err
		}

		auths[t] = auth
	}

	return &builder{
		auths: auths,
		cmd:   cmd,
		paths: paths,
	}, nil
}

func (b *builder) build(ctx context.Context) error {
	image, err := b.getBaseImage()
	if err != nil {
		return err
	}

	file, err := ioutil.TempFile("", "s3-sync")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	if err := createTarball(b.paths, file); err != nil {
		return err
	}

	layer, err := tarball.LayerFromFile(file.Name())
	if err != nil {
		return err
	}

	image, err = mutate.AppendLayers(image, layer)
	if err != nil {
		return err
	}

	image, err = mutate.CreatedAt(image, v1.Time{time.Now()})
	if err != nil {
		return err
	}

	for tag, a := range b.auths {
		auth := authnAuthenticatorFunc(func() (string, error) { return a.authorization(ctx) })
		if err := remote.Write(tag, image, remote.WithAuth(auth)); err != nil {
			return err
		}
	}

	return nil
}

func (b *builder) getBaseImage() (v1.Image, error) {
	if b.baseImage != nil {
		return b.baseImage, nil
	}

	layer, err := b.baseLayer()
	if err != nil {
		return nil, err
	}

	image, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		return nil, err
	}

	if b.baseImage, err = mutate.Config(image, b.config()); err != nil {
		return nil, err
	}

	return b.baseImage, nil
}

func (b *builder) baseLayer() (v1.Layer, error) {
	file, err := ioutil.TempFile("", "s3-sync")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if err := createTarball([]string{"/etc/ssl/certs/ca-certificates.crt", "/s3-sync"}, file); err != nil {
		return nil, err
	}

	return tarball.LayerFromFile(file.Name())
}

func (b *builder) config() v1.Config {
	config := v1.Config{}

	config.Volumes = make(map[string]struct{}, len(b.paths))
	for _, p := range b.paths {
		config.Volumes[p] = struct{}{}
	}

	config.Entrypoint = []string{"/s3-sync"}
	config.Cmd = b.cmd

	return config
}

func createTarball(paths []string, w io.Writer) error {
	writer := tar.NewWriter(w)
	defer writer.Close()

	added := make(map[string]bool)

	for _, path := range paths {
		path = strings.TrimPrefix(filepath.Clean(path), string(os.PathSeparator))

		elements := strings.Split(path, string(os.PathSeparator))
		for i := range elements {
			p := filepath.Join(elements[:i+1]...)
			if added[p] {
				continue
			}

			info, err := os.Lstat(p)
			if err != nil {
				return err
			}

			if err := addToTarball(writer, p, info); err != nil {
				return err
			}
			added[p] = true
		}

		err := filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if added[p] {
				return nil
			}

			if err := addToTarball(writer, p, info); err != nil {
				return err
			}
			added[p] = true

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func addToTarball(writer *tar.Writer, path string, info os.FileInfo) error {
	var link string
	var err error
	if info.Mode()&os.ModeSymlink != 0 {
		if link, err = os.Readlink(path); err != nil {
			return err
		}
	}

	header, err := tar.FileInfoHeader(info, link)
	if err != nil {
		return err
	}
	header.Name = path
	if info.IsDir() {
		header.Name += string(os.PathSeparator)
		header.ModTime = time.Time{}
	}

	if err := writer.WriteHeader(header); err != nil {
		return err
	}

	if !info.Mode().IsRegular() {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := io.Copy(writer, file); err != nil {
		return err
	}

	return nil
}

type keychain struct {
	awsClientFactory awsClientFactory
}

func (k *keychain) resolve(registry name.Registry) (authenticator, error) {
	auth, err := authn.DefaultKeychain.Resolve(registry)
	if err != nil {
		return nil, err
	}

	if auth != authn.Anonymous {
		return authenticatorFunc(func(ctx context.Context) (string, error) {
			return auth.Authorization()
		}), nil
	}

	r := regexp.MustCompile(`\A(\d+)\.dkr\.ecr\.([0-9a-z-]+)\.amazonaws\.com(?:\.cn)?\z`)
	matches := r.FindStringSubmatch(registry.Name())
	if matches == nil {
		return authenticatorFunc(func(ctx context.Context) (string, error) {
			return authn.Anonymous.Authorization()
		}), nil
	}

	registryID, region := matches[1], matches[2]
	return &ecrAuthenticator{ecrApi: k.awsClientFactory.newECR(region), registryID: registryID}, nil
}

type authnAuthenticatorFunc func() (string, error)

// Authorization implements authn.Authenticator
func (f authnAuthenticatorFunc) Authorization() (string, error) {
	return f()
}

type authenticator interface {
	authorization(ctx context.Context) (string, error)
}

type authenticatorFunc func(ctx context.Context) (string, error)

func (f authenticatorFunc) authorization(ctx context.Context) (string, error) {
	return f(ctx)
}

type ecrAuthenticator struct {
	basic       *authn.Basic
	ecrApi      ecriface.ECRAPI
	validBefore time.Time
	registryID  string
}

func (a *ecrAuthenticator) authorization(ctx context.Context) (string, error) {
	if a.basic != nil && time.Now().Before(a.validBefore) {
		return a.basic.Authorization()
	}

	token, expiresAt, err := a.getAuthorizationToken(ctx)
	if err != nil {
		return "", err
	}

	parts := strings.SplitN(string(token), ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid authorization token: %s", token)
	}

	a.basic = &authn.Basic{Username: parts[0], Password: parts[1]}
	a.validBefore = expiresAt.Add(-1 * expiresAt.Sub(time.Now()) / time.Duration(2))

	return a.basic.Authorization()
}

func (a *ecrAuthenticator) getAuthorizationToken(ctx context.Context) (string, time.Time, error) {
	input := &ecr.GetAuthorizationTokenInput{
		RegistryIds: []*string{aws.String(a.registryID)},
	}

	output, err := a.ecrApi.GetAuthorizationTokenWithContext(ctx, input)
	if err != nil {
		return "", time.Time{}, err
	}

	if len(output.AuthorizationData) != 1 {
		return "", time.Time{}, errors.New("no authorization token found")
	}
	data := output.AuthorizationData[0]

	token, err := base64.StdEncoding.DecodeString(aws.StringValue(data.AuthorizationToken))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("invalid authorization token: %v", err)
	}

	return string(token), aws.TimeValue(data.ExpiresAt), nil
}
