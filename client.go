package zooey

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3 struct {
	Client *s3.S3
}

func NewClient(accessKey, secretKey string) (*S3, error) {
	creds := credentials.NewStaticCredentials(accessKey, secretKey, "")
	sess, err := session.NewSession(&aws.Config{
		Credentials: creds,
		Region:      aws.String("ap-northeast-1")})
	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)
	c := &S3{
		Client: svc,
	}

	return c, err
}
