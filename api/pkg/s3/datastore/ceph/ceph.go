// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ceph

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/credentials"

	//"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/click2cloud-alpha/s3client"
	. "github.com/click2cloud-alpha/s3client"
	"github.com/click2cloud-alpha/s3client/models"
	"github.com/micro/go-log"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
)

type CephAdapter struct {
	backend *backendpb.BackendDetail
	session *s3client.Client
}

type CephAWSHelper struct {
	accessKey string
	secretKey string
	endPoint  string
}

type s3Cred struct {
	ak string
	sk string
}

func (myc *s3Cred) Retrieve() (credentials.Value, error) {
	cred := credentials.Value{AccessKeyID: myc.ak, SecretAccessKey: myc.sk}
	return cred, nil
}

func (myc *s3Cred) IsExpired() bool {
	return false
}

func Init(backend *backendpb.BackendDetail) *CephAdapter {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	sess := s3client.NewClient(endpoint, AccessKeyID, AccessKeySecret)
	adap := &CephAdapter{backend: backend, session: sess}
	return adap
}

func md5Content(data []byte) string {
	md5Ctx := md5.New()
	md5Ctx.Write(data)
	cipherStr := md5Ctx.Sum(nil)
	value := base64.StdEncoding.EncodeToString(cipherStr)
	return value
}

//This function is used to get object contain type
func getFileContentTypeCephOrAWS(out []byte) (string, error) {

	// Only the first 512 bytes are used to sniff the content type.
	buffer := make([]byte, 512)

	_ = len(buffer)

	// Use the net/http package's handy DectectContentType function. Always returns a valid
	// content-type by returning "application/octet-stream" if no others seemed to match.
	contentType := http.DetectContentType(buffer)

	return contentType, nil
}

func (ad *CephAdapter) PUT(stream io.Reader, object *pb.Object, ctx context.Context) S3Error {
	//bucketName := ad.backend.BucketName
	//
	//newObjectKey := object.BucketName + "/" + object.ObjectKey

	if ctx.Value("operation") == "upload" {
		bucket := ad.session.NewBucket()

		ceph_object := bucket.NewObject(ad.backend.BucketName)

		d, err := ioutil.ReadAll(stream)
		data := []byte(d)
		md5 := md5Content(data)
		contentType, err := getFileContentTypeCephOrAWS(data)

		length := int64(len(d))
		body := ioutil.NopCloser(bytes.NewReader(data))

		err = ceph_object.Create(object.ObjectKey, md5, string(contentType), length, body, models.PublicReadWrite)
		if err != nil {
			s3error := S3Error{501, err.Error()}

			return s3error
		}
		//uploader := s3manager.NewUploader(ad.session)
		//_, err := uploader.Upload(&s3manager.UploadInput{
		//	Bucket: &bucket,
		//	Key:    &newObjectKey,
		//	Body:   stream,
		//})
		//
		//if err != nil {
		//	log.Logf("Upload to aws failed:%v", err)
		//	return S3Error{Code: 500, Description: "Upload to aws failed"}
		//} else {
		//	object.LastModified = time.Now().String()[:19]
		//	log.Logf("LastModified is:%v\n", object.LastModified)
		//}

	}

	return NoError
}

func (ad *CephAdapter) GET(object *pb.Object, context context.Context, start int64, end int64) (io.ReadCloser, S3Error) {
	strStart := ""
	strEnd := ""
	if start != 0 || end != 0 {
		strStart = strconv.FormatInt(start, 10)
		strEnd = strconv.FormatInt(end, 10)
		//rangestr := "bytes=" + strStart + "-" + strEnd
		//getObjectInput.SetRange(rangestr)
	}
	if strStart != "" {
		i, err := strconv.Atoi(strStart)
		if err != nil {
			fmt.Println(err)
		}
		start = int64(i)
	}
	if strEnd != "" {
		i, err := strconv.Atoi(strEnd)
		if err != nil {
			fmt.Println(err)
		}
		end = int64(i)
	}

	range_obj := Range{start,
		end,
	}
	getObjectOpetion := GetObjectOption{
		Range: &range_obj,
	}
	if context.Value("operation") == "download" {
		bucket := ad.session.NewBucket()

		ceph_object := bucket.NewObject(ad.backend.BucketName)

		get_object, err := ceph_object.Get(object.ObjectKey, &getObjectOpetion)
		if err != nil {
			fmt.Println(err)
			log.Logf("Download failed:%v", err)
			return nil, S3Error{Code: 500, Description: "Download failed"}
		} else {
			log.Logf("Download succeed, bytes:%d\n", get_object.ContentLength)
			//defer get_object.Body.Close()
			//		body := bytes.NewReader(get_object.Body)
			//		ioReaderClose := ioutil.NopCloser(body)
			return get_object.Body, NoError
		}
	}

	return nil, NoError
}

func (ad *CephAdapter) DELETE(object *pb.DeleteObjectInput, ctx context.Context) S3Error {

	bucket := ad.session.NewBucket()

	ceph_object := bucket.NewObject(ad.backend.BucketName)

	err := ceph_object.Remove(object.Key)
	//bucket := ad.backend.BucketName
	//
	//newObjectKey := object.Bucket + "/" + object.Key
	//
	//deleteInput := awss3.DeleteObjectInput{Bucket: &bucket, Key: &newObjectKey}
	//
	//svc := awss3.New(ad.session)
	//_, err := svc.DeleteObject(&deleteInput)
	if err != nil {
		log.Logf("Delete object failed, err:%v\n", err)
		return InternalError
	}

	log.Logf("Delete object %s from aws successfully.\n", object.Key)

	return NoError
}

func (ad *CephAdapter) GetObjectInfo(bucketName string, key string, context context.Context) (*pb.Object, S3Error) {
	bucket := ad.session.NewBucket()
	object := bucket.NewObject(bucketName)
	resp, err := object.GetHeader(key, nil)
	if err != nil {
		log.Fatalf("Error occured during get Object Info, err:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}
	} else {
		objectInfo := &pb.Object{
			BucketName: bucketName,
			ObjectKey:  key,
			Size:       resp.ContentLength,
		}
		//fmt.Println(resp)
		//fmt.Println(resp.Header.Get("Content-Type"))
		return objectInfo, NoError
	}

	log.Logf("Can not find spceified object(%s).\n", key)
	return nil, NoSuchObject
}

func (ad *CephAdapter) InitMultipartUpload(object *pb.Object, context context.Context) (*pb.MultipartUpload, S3Error) {
	bucket := ad.session.NewBucket()
	ceph_object := bucket.NewObject(ad.backend.BucketName)
	uploader := ceph_object.NewUploads(object.ObjectKey)
	multipartUpload := &pb.MultipartUpload{}

	res, err := uploader.Initiate(nil)

	if err != nil {
		s3error := S3Error{500, err.Error()}

		return nil, s3error
	} else {
		//sample.UploadId = res.UploadID
		multipartUpload.Bucket = ad.backend.BucketName
		multipartUpload.Key = object.ObjectKey
		multipartUpload.UploadId = res.UploadID
		return multipartUpload, NoError
	}
}

func (ad *CephAdapter) UploadPart(stream io.Reader,
	multipartUpload *pb.MultipartUpload,
	partNumber int64, upBytes int64,
	context context.Context) (*model.UploadPartResult, S3Error) {
	tries := 1
	bucket := ad.session.NewBucket()
	ceph_object := bucket.NewObject(multipartUpload.Bucket)
	uploader := ceph_object.NewUploads(multipartUpload.Key)
	for tries <= 3 {

		//body := ioutil.NopCloser(bytes.NewReader(data))
		d, err := ioutil.ReadAll(stream)
		contain_type, err := getFileContentTypeCephOrAWS(d)
		data := []byte(d)
		body := ioutil.NopCloser(bytes.NewReader(data))
		md5 := md5Content(data)
		length := int64(len(data))
		part, err := uploader.UploadPart(int(partNumber), multipartUpload.UploadId, md5, contain_type, length, body)

		if err != nil {
			if tries == 3 {
				log.Logf("[ERROR]Upload part to aws failed. err:%v\n", err)
				return nil, S3Error{Code: 500, Description: "Upload failed"}
			}
			log.Logf("Retrying to upload part#%d ,err:%s\n", partNumber, err)
			tries++
		} else {
			log.Logf("Uploaded part #%d, ETag:%s\n", partNumber, part.Etag)
			result := &model.UploadPartResult{
				Xmlns:      model.Xmlns,
				ETag:       part.Etag,
				PartNumber: partNumber}
			return result, NoError
		}
	}
	//bucket := ad.backend.BucketName
	//newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	//bytess, _ := ioutil.ReadAll(stream)
	//upPartInput := &awss3.UploadPartInput{
	//	Body:          bytes.NewReader(bytess),
	//	Bucket:        &bucket,
	//	Key:           &newObjectKey,
	//	PartNumber:    aws.Int64(partNumber),
	//	UploadId:      &multipartUpload.UploadId,
	//	ContentLength: aws.Int64(upBytes),
	//}
	//log.Logf(">>>%v", upPartInput)
	//
	//svc := awss3.New(ad.session)
	//for tries <= 3 {
	//
	//	upRes, err := svc.UploadPart(upPartInput)
	//	if err != nil {
	//		if tries == 3 {
	//			log.Logf("[ERROR]Upload part to aws failed. err:%v\n", err)
	//			return nil, S3Error{Code: 500, Description: "Upload failed"}
	//		}
	//		log.Logf("Retrying to upload part#%d ,err:%s\n", partNumber, err)
	//		tries++
	//	} else {
	//		log.Logf("Uploaded part #%d, ETag:%s\n", partNumber, *upRes.ETag)
	//		result := &model.UploadPartResult{
	//			Xmlns:      model.Xmlns,
	//			ETag:       *upRes.ETag,
	//			PartNumber: partNumber}
	//		return result, NoError
	//	}
	//}
	return nil, NoError
}

func (ad *CephAdapter) CompleteMultipartUpload(multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload,
	context context.Context) (*model.CompleteMultipartUploadResult, S3Error) {

	bucket := ad.session.NewBucket()
	ceph_object := bucket.NewObject(multipartUpload.Bucket)
	uploader := ceph_object.NewUploads(multipartUpload.Key)
	var completeParts []CompletePart
	for _, p := range completeUpload.Part {
		completePart := CompletePart{
			Etag:       p.ETag,
			PartNumber: int(p.PartNumber),
		}
		completeParts = append(completeParts, completePart)
	}
	resp, err := uploader.Complete(multipartUpload.UploadId, completeParts)
	if err != nil {
		log.Logf("completeMultipartUploadS3 failed, err:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}
	}
	result := &model.CompleteMultipartUploadResult{
		Xmlns:    model.Xmlns,
		Location: ad.backend.Endpoint,
		Bucket:   multipartUpload.Bucket,
		Key:      multipartUpload.Key,
		ETag:     resp.Etag,
	}
	//bucket := ad.backend.BucketName
	//newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	//var completeParts []*awss3.CompletedPart
	//for _, p := range completeUpload.Part {
	//	completePart := &awss3.CompletedPart{
	//		ETag:       aws.String(p.ETag),
	//		PartNumber: aws.Int64(p.PartNumber),
	//	}
	//	completeParts = append(completeParts, completePart)
	//}
	//completeInput := &awss3.CompleteMultipartUploadInput{
	//	Bucket:   &bucket,
	//	Key:      &newObjectKey,
	//	UploadId: &multipartUpload.UploadId,
	//	MultipartUpload: &awss3.CompletedMultipartUpload{
	//		Parts: completeParts,
	//	},
	//}
	//log.Logf("completeInput %v\n", completeInput)
	//svc := awss3.New(ad.session)
	//resp, err := svc.CompleteMultipartUpload(completeInput)
	//if err != nil {
	//	log.Logf("completeMultipartUploadS3 failed, err:%v\n", err)
	//	return nil, S3Error{Code: 500, Description: err.Error()}
	//}
	//result := &model.CompleteMultipartUploadResult{
	//	Xmlns:    model.Xmlns,
	//	Location: *resp.Location,
	//	Bucket:   multipartUpload.Bucket,
	//	Key:      multipartUpload.Key,
	//	ETag:     *resp.ETag,
	//}

	log.Logf("completeMultipartUploadS3 successfully, resp:%v\n", resp)
	return result, NoError
}

func (ad *CephAdapter) AbortMultipartUpload(multipartUpload *pb.MultipartUpload, context context.Context) S3Error {
	//bucket := ad.backend.BucketName
	//newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	//abortInput := &awss3.AbortMultipartUploadInput{
	//	Bucket:   &bucket,
	//	Key:      &newObjectKey,
	//	UploadId: &multipartUpload.UploadId,
	//}
	//
	//svc := awss3.New(ad.session)
	//rsp, err := svc.AbortMultipartUpload(abortInput)

	bucket := ad.session.NewBucket()
	ceph_object := bucket.NewObject(multipartUpload.Bucket)
	uploader := ceph_object.NewUploads(multipartUpload.Key)
	err := uploader.RemoveUploads(multipartUpload.UploadId)

	if err != nil {
		log.Logf("abortMultipartUploadS3 failed, err:%v\n", err)
		return S3Error{Code: 500, Description: err.Error()}
	} else {
		log.Logf("abortMultipartUploadS3 successfully\n")
	}
	return NoError
}

func (ad *CephAdapter) ListParts(listParts *pb.ListParts, context context.Context) (*model.ListPartsOutput, S3Error) {
	bucket := ad.session.NewBucket()
	ceph_object := bucket.NewObject(listParts.Bucket)
	uploader := ceph_object.NewUploads(listParts.Key)

	value1, err := uploader.ListPart(listParts.UploadId)
	if err != nil {
		fmt.Println("Error Occured")
		fmt.Println(err)
	} else {
		fmt.Println(value1)
	}
	return nil, NoError
}
