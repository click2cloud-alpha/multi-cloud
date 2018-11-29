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

package cephs3mover

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"errors"
	. "github.com/click2cloud-alpha/s3client"
	"github.com/click2cloud-alpha/s3client/models"

	"io/ioutil"
	"net/http"

	//"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/credentials"
	//"github.com/aws/aws-sdk-go/aws/session"
	//"github.com/aws/aws-sdk-go/service/s3"
	//"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	pb "github.com/opensds/multi-cloud/datamover/proto"
	"io"
)

// DefaultDownloadPartSize is the default range of bytes to get at a time when
// using Download or Upload
const DefaultDownloadUploadPartSize = 1024 * 1024 * 5

// DefaultDownloadConcurrency is the default number of goroutines to spin up
// when using Download or Upload
const DefaultDownloadUploadConcurrency = 5

type S3Error struct {
	Code        int
	Description string
}

type FailPart struct {
	partNumber  int
	uploadId    string
	md5         string
	containType string
	body        io.ReadCloser
	length      int64
}

type CephS3Helper struct {
	accessKey string
	secretKey string
	endPoint  string
}

type Value struct {
	// AWS Access key ID
	AccessKeyID string

	// AWS Secret Access Key
	SecretAccessKey string

	// AWS Session Token
	Endpoint string
}

func (cephS3Helper *CephS3Helper) createClient() *Client {

	client := NewClient(cephS3Helper.endPoint, cephS3Helper.accessKey, cephS3Helper.secretKey)

	return client
}

// The Downloader structure that calls Download(). It is safe to call Download()
// on this structure for multiple objects and across concurrent goroutines.
// Mutating the Downloader's properties is not safe to be done concurrently.
type Downloader struct {
	// The buffer size (in bytes) to use when buffering data into chunks and
	// sending them as parts to S3. The minimum allowed part size is 5MB, and
	// if this value is set to zero, the DefaultDownloadPartSize value will be used.
	//
	// PartSize is ignored if the Range input parameter is provided.
	PartSize int64

	// The number of goroutines to spin up in parallel when sending parts.
	// If this is set to zero, the DefaultDownloadConcurrency value will be used.
	//
	// Concurrency of 1 will download the parts sequentially.
	//
	// Concurrency is ignored if the Range input parameter is provided.
	Concurrency int

	// An Ceph S3 client to use when performing downloads.
	CephS3 Client
}

type CreateMultipartUploadOutput struct {
	UploadID string
}

//
type CephS3Mover struct {
	downloader         *Downloader                  //for multipart download
	svc                *Uploads                     //for multipart upload
	multiUploadInitOut *CreateMultipartUploadOutput //for multipart upload
	//uploadId string //for multipart upload
	completeParts []*CompletePart //for multipart upload
}

//type s3Cred struct {
//	ak string
//	sk string
//}

func (myc *CephS3Helper) Retrieve() (Value, error) {
	cred := Value{
		AccessKeyID:     myc.accessKey,
		SecretAccessKey: myc.secretKey,
		Endpoint:        myc.endPoint,
	}
	return cred, nil
}

func (myc *CephS3Helper) IsExpired() bool {
	return false
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

	_ = len(out)

	// Use the net/http package's handy DectectContentType function. Always returns a valid
	// content-type by returning "application/octet-stream" if no others seemed to match.
	contentType := http.DetectContentType(buffer)

	return contentType, nil
}

func (mover *CephS3Mover) UploadObj(objKey string, destLoca *LocationInfo, buf []byte) error {
	log.Logf("[cephs3mover] UploadObj object, key:%s.", objKey)
	//s3c := s3Cred{ak: destLoca.Access, sk: destLoca.Security}
	//creds := credentials.NewCredentials(&s3c)
	//sess := &session.Session{}
	//err:=errors.New("")
	//if destLoca.Region == "" {
	//	sess, err = session.NewSession(&aws.Config{
	//
	//		Endpoint:    aws.String(destLoca.EndPoint),
	//		Credentials: creds,
	//	})
	//}else {
	//	sess, err = session.NewSession(&aws.Config{
	//		Region:      aws.String(destLoca.Region),
	//		Endpoint:    aws.String(destLoca.EndPoint),
	//		Credentials: creds,
	//	})
	//}

	sess := NewClient(destLoca.EndPoint, destLoca.Access, destLoca.Security)
	bucket := sess.NewBucket()

	cephObject := bucket.NewObject(destLoca.BucketName)

	md5 := md5Content(buf)

	contentType, err := getFileContentTypeCephOrAWS(buf)

	length := int64(len(buf))

	data := []byte(buf)
	body := ioutil.NopCloser(bytes.NewReader(data))
	log.Logf("[cephs3mover] Try to upload, bucket:%s,obj:%s\n", destLoca.BucketName, objKey)

	err = cephObject.Create(objKey, md5, string(contentType), length, body, models.BucketOwnerFull)

	if err != nil {
		log.Logf("[cephs3mover] Upload object[%s] failed, err:%v\n", objKey, err)
		//s3error := S3Error{501, err.Error()}

		//return err
	} else {
		log.Logf("[cephs3mover] Upload object[%s] successfully.", objKey)
	}

	//reader := bytes.NewReader(buf)
	//uploader := s3manager.NewUploader(sess)
	//log.Logf("[cephs3mover] Try to upload, bucket:%s,obj:%s\n", destLoca.BucketName, objKey)
	//_, err = uploader.Upload(&s3manager.UploadInput{
	//	Bucket: aws.String(destLoca.BucketName),
	//	Key:    aws.String(objKey),
	//	Body:   reader,
	//})
	//if err != nil {
	//	log.Logf("[cephs3mover] Upload object[%s] failed, err:%v\n", objKey, err)
	//} else {
	//	log.Logf("[cephs3mover] Upload object[%s] successfully.", objKey)
	//}

	return err
}

func (mover *CephS3Mover) DownloadObj(objKey string, srcLoca *LocationInfo, buf []byte) (size int64, err error) {
	log.Logf("[cephs3mover] DownloadObj object, key:%s.", objKey)
	sess := NewClient(srcLoca.EndPoint, srcLoca.Access, srcLoca.Security)
	bucket := sess.NewBucket()
	cephObject := bucket.NewObject(srcLoca.BucketName)
	res, err := cephObject.Get(objKey, nil)
	numBytes := int64(0)
	if err != nil {
		log.Logf("[cephs3mover]download object[bucket:%s,key:%s] failed, err:%v\n", srcLoca.BucketName, objKey, err)
	} else {
		numBytes = res.ContentLength
		log.Logf("[cephs3mover]downlad object[bucket:%s,key:%s] succeed, bytes:%d\n", srcLoca.BucketName, objKey, numBytes)
	}

	return numBytes, err

}

func (mover *CephS3Mover) MultiPartDownloadInit(srcLoca *LocationInfo) error {

	var err error
	sess := NewClient(srcLoca.EndPoint, srcLoca.Access, srcLoca.Security)
	download := &Downloader{
		CephS3:      *sess,
		Concurrency: DefaultDownloadUploadConcurrency,
		PartSize:    DefaultDownloadUploadPartSize,
	}
	mover.downloader = download

	if err != nil {
		log.Logf("[obsmover] MultiPartDownloadInit failed:%v\n", err)
	}
	return err
}

func (mover *CephS3Mover) DownloadRange(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64, err error) {
	log.Logf("[cephs3mover] Download object[%s] range[%d - %d]...\n", objKey, start, end)

	bucket := mover.downloader.CephS3.NewBucket()
	cephObject := bucket.NewObject(srcLoca.BucketName)

	getObjectOption := GetObjectOption{}
	if start != int64(0) || end != int64(0) {
		rangeObj := Range{start,
			end,
		}
		getObjectOption = GetObjectOption{
			Range: &rangeObj,
		}
	}

	getObject, err := cephObject.Get(objKey, &getObjectOption)
	size = getObject.ContentLength
	defer getObject.Body.Close()
	d, err := ioutil.ReadAll(getObject.Body)
	if err != nil {
		log.Fatal(err)
	}
	buf = []byte(d)

	if err != nil {
		log.Logf("Download object[%s] range[%d - %d] faild, err:%v\n", objKey, start, end, err)
	} else {
		log.Logf(" Download object[%s] range[%d - %d] succeed, bytes:%d\n", objKey, start, end, size)
	}
	return size, err

}

func (mover *CephS3Mover) MultiPartUploadInit(objKey string, destLoca *LocationInfo) error {
	sess := NewClient(destLoca.EndPoint, destLoca.Access, destLoca.Security)
	bucket := sess.NewBucket()
	cephObject := bucket.NewObject(destLoca.BucketName)
	mover.svc = cephObject.NewUploads(objKey)

	//s3c := s3Cred{ak: destLoca.Access, sk: destLoca.Security}
	//creds := credentials.NewCredentials(&s3c)
	//sess, err := session.NewSession(&aws.Config{
	//	Region:      aws.String(destLoca.Region),
	//	Endpoint:    aws.String(destLoca.EndPoint),
	//	Credentials: creds,
	//})
	//if err != nil {
	//	log.Logf("[cephs3mover] New session failed, err:%v\n", err)
	//	return err
	//}
	//
	//mover.svc = s3.New(sess)
	//multiUpInput := &s3.CreateMultipartUploadInput{
	//	Bucket: aws.String(destLoca.BucketName),
	//	Key:    aws.String(objKey),
	//}
	//resp, err := mover.svc.CreateMultipartUpload(multiUpInput)
	log.Logf("[s3mover] Try to init multipart upload[objkey:%s].\n", objKey)
	for tries := 1; tries <= 3; tries++ {
		resp, err := mover.svc.Initiate(nil)
		if err != nil {
			log.Logf("[s3mover] Init multipart upload[objkey:%s] failed %d times.\n", objKey, tries)
			if tries == 3 {
				return err
			}
		} else {
			mover.multiUploadInitOut = &CreateMultipartUploadOutput{resp.UploadID}
			log.Logf("[s3mover] Init multipart upload[objkey:%s] successfully, UploadId:%s\n", objKey, resp.UploadID)
			return nil
		}
	}
	cephObject.SetACL(objKey, models.PublicReadWrite)

	//log.Logf("[s3mover] Init multipart upload[objkey:%s], should not be here.\n", objKey)
	//return errors.New("internal error")
	//if err != nil {
	//	log.Logf("[cephs3mover] Init multipart upload[objkey:%s] failed, err:%v\n", objKey, err)
	//	return errors.New("[cephs3mover] Init multipart upload failed.")
	//} else {
	//	log.Logf("[cephs3mover] Init multipart upload[objkey:%s] succeed, UploadId:%s\n", objKey, resp.UploadID)
	//}
	//
	////mover.uploadId = *resp.UploadId
	//mover.multiUploadInitOut = &CreateMultipartUploadOutput{resp.UploadID}
	log.Logf("[s3mover] Init multipart upload[objkey:%s], should not be here.\n", objKey)
	return errors.New("internal error")

}

func (mover *CephS3Mover) UploadPart(objKey string, destLoca *LocationInfo, upBytes int64, buf []byte, partNumber int64, offset int64) error {
	log.Logf("[cephs3mover] Upload range[objkey:%s, partnumber#%d,offset#%d,upBytes#%d,uploadid#%s]...\n", objKey, partNumber,
		offset, upBytes, mover.multiUploadInitOut.UploadID)
	//tries := 1
	//sess := NewClient(destLoca.EndPoint, destLoca.Access, destLoca.Security)
	//bucket := sess.NewBucket()
	//cephObject := bucket.NewObject(destLoca.BucketName)
	//destUploader := cephObject.NewUploads(objKey)
	//upPartInput := &s3.UploadPartInput{
	//	Body:          bytes.NewReader(buf),
	//	Bucket:        aws.String(destLoca.BucketName),
	//	Key:           aws.String(objKey),
	//	PartNumber:    aws.Int64(partNumber),
	//	UploadId:      aws.String(*mover.multiUploadInitOut.UploadId),
	//	ContentLength: aws.Int64(upBytes),
	//}
	md5 := md5Content(buf)
	contentType, err := getFileContentTypeCephOrAWS(buf)
	if err != nil {
		log.Logf("[cephs3mover]. err:%v\n", err)
	}

	length := int64(len(buf))

	data := []byte(buf)
	body := ioutil.NopCloser(bytes.NewReader(data))

	for tries := 1; tries <= 3; tries++ {
		upRes, err := mover.svc.UploadPart(int(partNumber), mover.multiUploadInitOut.UploadID, md5, contentType, length, body)
		if err != nil {
			log.Logf("[s3mover] Upload range[objkey:%s, partnumber#%d, offset#%d] failed %d times, err:%v\n",
				objKey, partNumber, offset, tries, err)
			if tries == 3 {
				return err
			}
		} else {

			//part := s3client.CompletePart{Etag: upRes.Etag, PartNumber:upRes.PartNumber}

			//mover.completeParts = part

			mover.completeParts = append(mover.completeParts, upRes)
			log.Logf("[s3mover] Upload range[objkey:%s, partnumber#%d,offset#%d] successfully.\n", objKey, partNumber, offset)
			return nil
		}
	}
	log.Logf("[s3mover] Upload range[objkey:%s, partnumber#%d, offset#%d], should not be here.\n", objKey, partNumber, offset)
	return errors.New("internal error")
}

func (mover *CephS3Mover) AbortMultipartUpload(objKey string, destLoca *LocationInfo) error {
	log.Logf("[cephs3mover] Aborting multipart upload[objkey:%s] for uploadId#%s.\n", objKey, mover.multiUploadInitOut.UploadID)
	bucket := mover.downloader.CephS3.NewBucket()
	cephObject := bucket.NewObject(destLoca.BucketName)
	uploader := cephObject.NewUploads(objKey)
	err := uploader.RemoveUploads(mover.multiUploadInitOut.UploadID)
	if err != nil {
		log.Logf("abortMultipartUpload failed, err:%v\n", err)
		return err
	} else {
		log.Logf("abortMultipartUpload successfully\n")
	}
	return nil
}

func (mover *CephS3Mover) CompleteMultipartUpload(objKey string, destLoca *LocationInfo) error {
	//sess := NewClient(destLoca.EndPoint, destLoca.Access, destLoca.Security)
	//bucket := sess.NewBucket()
	//cephObject := bucket.NewObject(destLoca.BucketName)
	//destUploader := cephObject.NewUploads(objKey)

	//completeInput := &s3.CompleteMultipartUploadInput{
	//	Bucket:   aws.String(destLoca.BucketName),
	//	Key:      aws.String(objKey),
	//	UploadId: aws.String(*mover.multiUploadInitOut.UploadId),
	//	MultipartUpload: &s3.CompletedMultipartUpload{
	//		Parts: mover.completeParts,
	//	},
	//}

	log.Logf("[s3mover] Try to do CompleteMultipartUpload [objkey:%s].\n", objKey)
	for tries := 1; tries <= 3; tries++ {
		var completeParts []CompletePart
		for _, p := range mover.completeParts {
			completePart := CompletePart{
				Etag:       p.Etag,
				PartNumber: int(p.PartNumber),
			}
			completeParts = append(completeParts, completePart)
		}
		rsp, err := mover.svc.Complete(mover.multiUploadInitOut.UploadID, completeParts)
		if err != nil {
			log.Logf("[s3mover] completeMultipartUpload [objkey:%s] failed %d times, err:%v\n", objKey, tries, err)
			if tries == 3 {
				return err
			}
		} else {
			log.Logf("[s3mover] completeMultipartUpload successfully [objkey:%s], rsp:%v\n", objKey, rsp)
			return nil
		}
	}

	//completeInput := &s3.CompleteMultipartUploadInput{
	//	Bucket:   aws.String(destLoca.BucketName),
	//	Key:      aws.String(objKey),
	//	UploadId: aws.String(*mover.multiUploadInitOut.UploadId),
	//	MultipartUpload: &s3.CompletedMultipartUpload{
	//		Parts: mover.completeParts,
	//	},
	//}
	//
	//rsp, err := mover.svc.CompleteMultipartUpload(completeInput)
	//if err != nil {
	//	log.Logf("[cephs3mover] completeMultipartUploadS3 failed [objkey:%s], err:%v\n", objKey, err)
	//} else {
	//	log.Logf("[cephs3mover] completeMultipartUploadS3 successfully [objkey:%s], rsp:%v\n", objKey, rsp)
	//}
	//
	//return err
	log.Logf("[s3mover] completeMultipartUpload [objkey:%s], should not be here.\n", objKey)
	return errors.New("internal error")
}

func (mover *CephS3Mover) DeleteObj(objKey string, loca *LocationInfo) error {

	sess := NewClient(loca.EndPoint, loca.Access, loca.Security)
	bucket := sess.NewBucket()

	cephObject := bucket.NewObject(loca.BucketName)

	err := cephObject.Remove(objKey)

	//s3c := s3Cred{ak: loca.Access, sk: loca.Security}
	//creds := credentials.NewCredentials(&s3c)
	//sess, err := session.NewSession(&aws.Config{
	//	Region:      aws.String(loca.Region),
	//	Endpoint:    aws.String(loca.EndPoint),
	//	Credentials: creds,
	//})
	//if err != nil {
	//	log.Logf("[cephs3mover] New session failed, err:%v\n", err)
	//	return err
	//}
	//
	//svc := s3.New(sess)
	//_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(loca.BucketName), Key: aws.String(objKey)})
	//if err != nil {
	//	log.Logf("[cephs3mover] Unable to delete object[key:%s] from bucket %s, %v\n", objKey, loca.BucketName, err)
	//}
	//
	//err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
	//	Bucket: aws.String(loca.BucketName),
	//	Key:    aws.String(objKey),
	//})
	if err != nil {
		log.Logf("[cephs3mover] Error occurred while waiting for object[%s] to be deleted.\n", objKey)
	}

	log.Logf("[cephs3mover] Delete Object[%s] successfully.\n", objKey)
	return err
}

func ListObjs(loca *LocationInfo, filt *pb.Filter) ([]models.GetBucketResponseContent, error) {

	sess := NewClient(loca.EndPoint, loca.Access, loca.Security)
	bucket := sess.NewBucket()

	//cephObject := bucket.NewObject(loca.BucketName)
	//bucket := connection.NewBucket()
	getBucket, err := bucket.Get(string(loca.BucketName), "", "", "", 1000)

	if filt != nil {
		getBucket.Prefix = filt.Prefix
	}

	output := getBucket.Contents //.ListObjects(input)
	if err != nil {
		log.Logf("[cephs3mover] Error occurred while waiting for object", err)
		return getBucket.Contents, nil
	}
	//listObject :=getBucket.Contents
	ObjectCounts := len(output)
	//fmt.Println("list of bucks, %#v", no_object)
	for i := 0; i < ObjectCounts; i++ {
		resp := output[i]
		//getBucket.Marker = output.NextMarker
		//output, err = svc.ListObjects(input)
		output = append(output, resp)

		//for i := 0; i < ObjectCounts; i++ {
		//	fmt.Println(listObject2[i].Key)
		//	//fmt.Println(listObject2[i].LastModified)
		//	//fmt.Println(listObject2[i].Owner)
		//	fmt.Println("Size")
		//	fmt.Println(listObject2[i].Size)
		//	//fmt.Println(listObject2[i].StorageClass)
		//}
		//if err != nil {
		//	fmt.Println(err)
		//}
		//
		//input := &s3.ListObjectsInput{Bucket: aws.String(loca.BucketName)}
		//if filt != nil {
		//	input.Prefix = &filt.Prefix
		//}
		//output, e := svc.ListObjects(input)
		//if e != nil {
		//	log.Logf("[cephs3mover] List aws bucket failed, err:%v\n", e)
		//	return nil, e
		//}
		//
		//objs := output.Contents
		//for ; *output.IsTruncated == true; {
		//	input.Marker = output.NextMarker
		//	output, err = svc.ListObjects(input)
		//	if err != nil {
		//		log.Logf("[cephs3mover] List objects failed, err:%v\n", err)
		//		return nil, err
		//	}
		//	objs = append(objs, output.Contents...)
		//}

	}
	log.Logf("[cephs3mover] Number of objects in bucket[%s] is %d.\n", loca.BucketName, len(output))
	return output, nil
}
