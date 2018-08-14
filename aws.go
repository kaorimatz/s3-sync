package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ecr"
	"github.com/aws/aws-sdk-go/service/ecr/ecriface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type awsClientFactory interface {
	newECR(region string) ecriface.ECRAPI
	newS3(region string) s3iface.S3API
}

type defaultAWSClientFactory struct {
	session *session.Session
	config  *aws.Config
}

func newDefaultAWSClientFactory() (*defaultAWSClientFactory, error) {
	session, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	return &defaultAWSClientFactory{
		session: session,
		config:  aws.NewConfig(),
	}, nil
}

func (f *defaultAWSClientFactory) newECR(region string) ecriface.ECRAPI {
	return ecr.New(f.session, f.config.WithRegion(region))
}

func (f *defaultAWSClientFactory) newS3(region string) s3iface.S3API {
	return s3.New(f.session, f.config.WithRegion(region))
}
