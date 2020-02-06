// Copyright 2020 The goftp Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package server

import (
	"errors"
	"io"
	"os"
	"strings"
	"time"

	minio "github.com/minio/minio-go/v6"
)

var (
	ErrNotImplemented = errors.New("not implemented")
)

type MinioDriver struct {
	client *minio.Client
	perm   Perm
	bucket string
}

func (driver *MinioDriver) Init(conn *Conn) {
}

func (driver *MinioDriver) ChangeDir(path string) error {
	return ErrNotImplemented
}

func buildMinioPath(p string) string {
	return strings.TrimPrefix(p, "/")
}

type minioFileInfo struct {
	p     string
	info  minio.ObjectInfo
	perm  Perm
	isDir bool
}

func (m *minioFileInfo) Name() string {
	return m.p
}

func (m *minioFileInfo) Size() int64 {
	return m.info.Size
}

func (m *minioFileInfo) Mode() os.FileMode {
	mode, _ := m.perm.GetMode(m.p)
	return mode
}

func (m *minioFileInfo) ModTime() time.Time {
	return m.info.LastModified
}

func (m *minioFileInfo) IsDir() bool {
	return m.isDir
}

func (m *minioFileInfo) Sys() interface{} {
	return nil
}

func (m *minioFileInfo) Owner() string {
	owner, _ := m.perm.GetOwner(m.p)
	return owner
}

func (m *minioFileInfo) Group() string {
	group, _ := m.perm.GetGroup(m.p)
	return group
}

func (driver *MinioDriver) isDir(path string) (bool, error) {
	doneCh := make(chan struct{})
	defer close(doneCh)
	p := buildMinioPath(path)
	objectCh := driver.client.ListObjects(driver.bucket, p, false, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			return false, object.Err
		}
		return true, nil
	}
	return false, nil
}

func (driver *MinioDriver) Stat(path string) (FileInfo, error) {
	if path == "/" {
		return &minioFileInfo{
			p:     "/",
			perm:  driver.perm,
			isDir: true,
		}, nil
	}

	p := buildMinioPath(path)
	objInfo, err := driver.client.StatObject(driver.bucket, p, minio.StatObjectOptions{})
	if err != nil {
		if isDir, err := driver.isDir(path); err != nil {
			return nil, err
		} else if isDir {
			return &minioFileInfo{
				p:     path,
				perm:  driver.perm,
				isDir: true,
			}, nil
		}
		return nil, errors.New("Not a directory")
	}
	return &minioFileInfo{
		p:    p,
		info: objInfo,
		perm: driver.perm,
	}, nil
}

func (driver *MinioDriver) ListDir(path string, callback func(FileInfo) error) error {
	doneCh := make(chan struct{})
	defer close(doneCh)

	p := buildMinioPath(path)
	objectCh := driver.client.ListObjects(driver.bucket, p, false, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			return object.Err
		}

		if err := callback(&minioFileInfo{
			p:    object.Key,
			info: object,
			perm: driver.perm,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (driver *MinioDriver) DeleteDir(path string) error {
	doneCh := make(chan struct{})
	defer close(doneCh)

	p := buildMinioPath(path)
	objectCh := driver.client.ListObjects(driver.bucket, p, false, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			return object.Err
		}

		if err := driver.client.RemoveObject(driver.bucket, object.Key); err != nil {
			return err
		}
	}
	return nil
}

func (driver *MinioDriver) DeleteFile(path string) error {
	return driver.client.RemoveObject(driver.bucket, buildMinioPath(path))
}

func (driver *MinioDriver) Rename(fromPath string, toPath string) error {
	src := minio.NewSourceInfo(driver.bucket, buildMinioPath(fromPath), nil)
	dst, err := minio.NewDestinationInfo(driver.bucket, buildMinioPath(toPath), nil, nil)
	if err != nil {
		return err
	}

	if err := driver.client.CopyObject(dst, src); err != nil {
		return err
	}

	return driver.client.RemoveObject(driver.bucket, buildMinioPath(fromPath))
}

func (driver *MinioDriver) MakeDir(path string) error {
	return nil
}

func (driver *MinioDriver) GetFile(path string, offset int64) (int64, io.ReadCloser, error) {
	if offset > 0 {
		return 0, nil, ErrNotImplemented
	}
	object, err := driver.client.GetObject(driver.bucket, buildMinioPath(path), minio.GetObjectOptions{})
	if err != nil {
		return 0, nil, err
	}

	info, err := object.Stat()
	if err != nil {
		return 0, nil, err
	}

	return info.Size, object, nil
}

func (driver *MinioDriver) PutFile(destPath string, data io.Reader, appendData bool) (int64, error) {
	if !appendData {
		return driver.client.PutObject(driver.bucket, buildMinioPath(destPath), data, -1, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	}

	return 0, ErrNotImplemented
}

type MinioDriverFactory struct {
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	useSSL          bool
	location        string
	bucket          string
	perm            Perm
}

func NewMinioDriverFactory(endpoint, accessKeyID, secretAccessKey, location, bucket string, useSSL bool, perm Perm) *MinioDriverFactory {
	return &MinioDriverFactory{
		endpoint:        endpoint,
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		useSSL:          useSSL,
		location:        location,
		bucket:          bucket,
		perm:            perm,
	}
}

func (factory *MinioDriverFactory) NewDriver() (Driver, error) {
	// Initialize minio client object.
	minioClient, err := minio.New(factory.endpoint, factory.accessKeyID, factory.secretAccessKey, factory.useSSL)
	if err != nil {
		return nil, err
	}

	if err = minioClient.MakeBucket(factory.bucket, factory.location); err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(factory.bucket)
		if !exists || errBucketExists != nil {
			return nil, err
		}
	}

	return &MinioDriver{
		client: minioClient,
		bucket: factory.bucket,
		perm:   factory.perm,
	}, nil
}
