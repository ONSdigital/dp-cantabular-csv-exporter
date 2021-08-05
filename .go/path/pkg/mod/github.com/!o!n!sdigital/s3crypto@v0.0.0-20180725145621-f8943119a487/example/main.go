package main

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"os"

	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/s3crypto"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {

	f, err := ioutil.ReadFile("testdata/private.pem")
	if err != nil {
		panic(err)
	}

	block, _ := pem.Decode(f)
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		panic(err)
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		panic(err)
	}

	region := "eu-west-1"

	sess, err := session.NewSession(&aws.Config{Region: &region})
	if err != nil {
		panic(err)
	}

	size := 5 * 1024 * 1024
	svc := s3crypto.New(sess, &s3crypto.Config{PrivateKey: privateKey, MultipartChunkSize: size})

	bucket := "dp-frontend-florence-file-uploads"
	key := "cpicoicoptest.csv"

	b, err := ioutil.ReadFile("testdata/" + key)
	if err != nil {
		log.Error(err, nil)
		return
	}

	acl := "public-read"

	input := &s3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	}
	input.ACL = &acl

	result, err := svc.CreateMultipartUpload(input)
	if err != nil {
		log.ErrorC("error creating mpu", err, nil)
		return
	}

	log.Debug("created multi part upload", nil)

	chunks := split(b, size)

	var completedParts []*s3.CompletedPart

	for i, chunk := range chunks {

		partN := int64(i + 1)

		partInput := &s3.UploadPartInput{
			Body:       bytes.NewReader(chunk),
			Bucket:     &bucket,
			Key:        &key,
			PartNumber: &partN,
			UploadId:   result.UploadId,
		}

		res, err := svc.UploadPart(partInput)
		if err != nil {
			log.Error(err, nil)
			return
		}

		log.Info("part completed", log.Data{"part": partN})

		completedParts = append(completedParts, &s3.CompletedPart{
			PartNumber: &partN,
			ETag:       res.ETag,
		})

	}

	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: result.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	cr, err := svc.CompleteMultipartUpload(completeInput)
	if err != nil {
		log.Error(err, nil)
		return
	}

	log.Info("upload completed", log.Data{"result": cr})

	log.Info("now getting file...", nil)

	getInput := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}

	out, err := svc.GetObject(getInput)
	if err != nil {
		log.Error(err, nil)
		return
	}

	newf, err := os.Create("newcpi.csv")
	if err != nil {
		log.Error(err, nil)
		return
	}
	defer newf.Close()

	newB, err := ioutil.ReadAll(out.Body)
	if err != nil {
		log.Error(err, nil)
		return
	}

	if _, err := newf.Write(newB); err != nil {
		log.Error(err, nil)
		return
	}

}

func split(buf []byte, lim int) [][]byte {
	var chunk []byte
	chunks := make([][]byte, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:len(buf)])
	}
	return chunks
}
