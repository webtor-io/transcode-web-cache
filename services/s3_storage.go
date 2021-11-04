package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/urfave/cli"

	cs "github.com/webtor-io/common-services"

	log "github.com/sirupsen/logrus"
)

type S3Storage struct {
	bucket string
	cl     *cs.S3Client
}

const (
	awsBucketFlag = "aws-bucket"
)

func RegisterS3StorageFlags(c *cli.App) {
	c.Flags = append(c.Flags, cli.StringFlag{
		Name:   awsBucketFlag,
		Usage:  "AWS Bucket",
		Value:  "",
		EnvVar: "AWS_BUCKET",
	})
}

func NewS3Storage(c *cli.Context, cl *cs.S3Client) *S3Storage {
	return &S3Storage{
		bucket: c.String(awsBucketFlag),
		cl:     cl,
	}
}

func (s *S3Storage) GetContent(ctx context.Context, key string, path string) (io.ReadCloser, error) {
	key = key + path
	log.Infof("Fetching content key=%v bucket=%v", key, s.bucket)
	r, err := s.cl.Get().GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == s3.ErrCodeNoSuchKey {
			log.Infof("Content not found key=%v bucket=%v", key, s.bucket)
			return nil, nil
		}
		return nil, errors.Wrap(err, "Failed to fetch content")
	}
	return r.Body, nil
}

func (s *S3Storage) CheckDoneMarker(ctx context.Context, key string) (bool, *time.Time, error) {
	key = "done/" + key
	log.Infof("Check done marker bucket=%v key=%v", s.bucket, key)
	r, err := s.cl.Get().GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == s3.ErrCodeNoSuchKey {
			return false, nil, nil
		}
		return false, nil, errors.Wrapf(err, "Failed to check done marker bucket=%v key=%v", s.bucket, key)
	}
	return true, r.LastModified, nil
}

func (s *S3Storage) Touch(ctx context.Context, key string) (err error) {
	key = "touch/" + key
	log.Infof("Touching bucket=%v key=%v", s.bucket, key)
	_, err = s.cl.Get().PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(fmt.Sprintf("%v", time.Now().Unix()))),
	})
	if err != nil {
		return errors.Wrapf(err, "Failed to touch bucket=%v key=%v", s.bucket, key)
	}
	return
}
