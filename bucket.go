package zooey

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Bucket struct {
	service *S3
	name    string
}

func (s *S3) Bucket(name string) *Bucket {
	return &Bucket{
		service: s,
		name:    name,
	}
}

type ListObjectOptions func(*s3.ListObjectsInput)

func Delimiter(s string) ListObjectOptions {
	return func(o *s3.ListObjectsInput) { o.Delimiter = &s }
}

func Prefix(s string) ListObjectOptions {
	return func(o *s3.ListObjectsInput) { o.Prefix = &s }
}

func (b *Bucket) List(options ...ListObjectOptions) (*s3.ListObjectsOutput, error) {
	input := &s3.ListObjectsInput{
		Bucket: &b.name,
	}

	for _, o := range options {
		o(input)
	}

	resp, err := b.service.Client.ListObjects(input)
	if err != nil {
		return nil, err
	}

	return resp, err
}

type GetObjectOptions func(*s3.GetObjectInput)

func Range(s string) GetObjectOptions {
	return func(o *s3.GetObjectInput) { o.Range = &s }
}

func (b *Bucket) GetObject(key string, options ...GetObjectOptions) (*s3.GetObjectOutput, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(key),
	}

	for _, o := range options {
		o(input)
	}

	resp, err := b.service.Client.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				return nil, aerr
			default:
				return nil, aerr
			}
		}
		return nil, err
	}

	return resp, err
}

func (b *Bucket) PutObject(name string, buf *bytes.Buffer) (*s3.PutObjectOutput, error) {
	input := &s3.PutObjectInput{
		Body:   bytes.NewReader(buf.Bytes()),
		Bucket: &b.name,
		Key:    &name,
	}

	resp, err := b.service.Client.PutObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				return nil, aerr
			}
		}
		return nil, err
	}
	return resp, err
}

func (b *Bucket) IsExist(key string) bool {
	out, _ := b.List()
	for _, item := range out.Contents {
		if *item.Key == key {
			return true
		}
	}
	return false
}
