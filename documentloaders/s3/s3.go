package s3

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tmc/langchaingo/documentloaders"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/textsplitter"
)

const defaultRegion = "us-east-1"

// S3FileLoader loads text data from a file in a S3 bucket
type S3FileLoader struct {

	//S3 bucket name
	bucketName string

	//S3 object key
	key      string
	s3Client *s3.Client
}

var _ documentloaders.Loader = S3FileLoader{}

// NewS3FileLoader creates a new S3 file loader.
func NewS3FileLoader(bucketName, key string) S3FileLoader {

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = defaultRegion
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		log.Fatal(err)
	}

	return S3FileLoader{
		bucketName: bucketName,
		key:        key,
		s3Client:   s3.NewFromConfig(cfg),
	}
}

// Load reads from the io.Reader and returns a single document with the data.
func (l S3FileLoader) Load(_ context.Context) ([]schema.Document, error) {

	resp, err := l.s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(l.bucketName),
		Key:    aws.String(l.key),
	})
	if err != nil {
		return nil, err
		//log.Fatalf("unable to read file %s from bucket %s - %v", l.key, l.bucketName, err)
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		return nil, err
	}

	return []schema.Document{
		{
			PageContent: buf.String(),
			Metadata:    map[string]any{"key": l.key},
		},
	}, nil
}

// LoadAndSplit reads text data from the io.Reader and splits it into multiple
// documents using a text splitter.
func (l S3FileLoader) LoadAndSplit(ctx context.Context, splitter textsplitter.TextSplitter) ([]schema.Document, error) {
	docs, err := l.Load(ctx)
	if err != nil {
		return nil, err
	}

	return textsplitter.SplitDocuments(splitter, docs)
}
