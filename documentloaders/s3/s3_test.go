package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
)

// invokes S3 APIs. should have the right S3 permissions.
func TestLoadTextFile(t *testing.T) {

	bucketName := "test-bucket-" + strconv.FormatInt(time.Now().UnixMicro(), 10)
	key := "sample.txt"
	fileContent := "Hello, S3!"

	file, err := os.Open("testdata/test.txt")
	assert.Nil(t, err)
	defer file.Close()

	err = prepData(bucketName, key, file)
	defer cleanUp(bucketName, key)
	assert.Nil(t, err)

	s3FileLoader := NewS3FileLoader(bucketName, key)
	doc, err := s3FileLoader.Load(context.Background())

	assert.Nil(t, err)

	assert.Equal(t, 1, len(doc))
	assert.Equal(t, fileContent, doc[0].PageContent)
	//assert.Equal(t, key, doc[0].Metadata["key"])
}

func TestLoadPDFFile(t *testing.T) {

	bucketName := "test-bucket-" + strconv.FormatInt(time.Now().UnixMicro(), 10)
	key := "sample.pdf"
	fileContent := "Hello, S3! "

	file, err := os.Open("testdata/test_pdf.pdf")
	assert.Nil(t, err)
	defer file.Close()

	err = prepData(bucketName, key, file)
	defer cleanUp(bucketName, key)
	assert.Nil(t, err)

	s3FileLoader := NewS3FileLoader(bucketName, key)
	doc, err := s3FileLoader.Load(context.Background())

	assert.Nil(t, err)

	assert.Equal(t, 1, len(doc))
	assert.Equal(t, fileContent, doc[0].PageContent)
	//assert.Equal(t, key, doc[0].Metadata["key"])
}

func TestLoadInvalidFile(t *testing.T) {

	bucketName := "test-bucket-" + strconv.FormatInt(time.Now().UnixMicro(), 10)
	key := "foo.bar"
	fileContent := "Hello, S3!"

	err := prepData(bucketName, key, strings.NewReader(fileContent))
	defer cleanUp(bucketName, key)
	assert.Nil(t, err)

	s3FileLoader := NewS3FileLoader(bucketName, key)
	_, err = s3FileLoader.Load(context.Background())
	assert.NotNil(t, err)
}

func prepData(bucketName, key string, content io.Reader) error {

	client := getS3Client()

	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		return err
	}
	fmt.Println("Bucket created successfully!")

	_, err = client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   content,
	})
	if err != nil {
		return err
	}
	fmt.Println("File uploaded successfully!")

	return nil

}

func _prepData(bucketName, key, content string) error {

	client := getS3Client()

	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		return err
	}
	fmt.Println("Bucket created successfully!")

	_, err = client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   bytes.NewReader([]byte(content)),
	})
	if err != nil {
		return err
	}
	fmt.Println("File uploaded successfully!")

	return nil

}

func cleanUp(bucketName, key string) {

	client := getS3Client()

	_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		log.Fatalf("Unable to delete %q from %q, %v", key, bucketName, err)
	}
	fmt.Println("File deleted successfully!")

	// Delete the bucket
	_, err = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		log.Fatalf("Unable to delete bucket %q, %v", bucketName, err)
	}
	fmt.Println("Bucket deleted successfully!")
}

func getS3Client() *s3.Client {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		log.Fatal(err)
	}

	return s3.NewFromConfig(cfg)
}
