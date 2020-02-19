// Copyright 2020 The goftp Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package server

import (
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/stretchr/testify/assert"
)

type mockNotifier struct {
	actions []string
	lock    sync.Mutex
}

func (m *mockNotifier) BeforeLoginUser(conn *Conn, userName string) {
	m.lock.Lock()
	m.actions = append(m.actions, "BeforeLoginUser")
	m.lock.Unlock()
}
func (m *mockNotifier) BeforePutFile(conn *Conn, dstPath string) {
	m.lock.Lock()
	m.actions = append(m.actions, "BeforePutFile")
	m.lock.Unlock()
}
func (m *mockNotifier) BeforeDeleteFile(conn *Conn, dstPath string) {
	m.lock.Lock()
	m.actions = append(m.actions, "BeforeDeleteFile")
	m.lock.Unlock()
}
func (m *mockNotifier) BeforeChangeCurDir(conn *Conn, oldCurDir, newCurDir string) {
	m.lock.Lock()
	m.actions = append(m.actions, "BeforeChangeCurDir")
	m.lock.Unlock()
}
func (m *mockNotifier) BeforeCreateDir(conn *Conn, dstPath string) {
	m.lock.Lock()
	m.actions = append(m.actions, "BeforeCreateDir")
	m.lock.Unlock()
}
func (m *mockNotifier) BeforeDeleteDir(conn *Conn, dstPath string) {
	m.lock.Lock()
	m.actions = append(m.actions, "BeforeDeleteDir")
	m.lock.Unlock()
}
func (m *mockNotifier) BeforeDownloadFile(conn *Conn, dstPath string) {
	m.lock.Lock()
	m.actions = append(m.actions, "BeforeDownloadFile")
	m.lock.Unlock()
}
func (m *mockNotifier) AfterUserLogin(conn *Conn, userName, password string, passMatched bool, err error) {
	m.lock.Lock()
	m.actions = append(m.actions, "AfterUserLogin")
	m.lock.Unlock()
}
func (m *mockNotifier) AfterFilePut(conn *Conn, dstPath string, size int64, err error) {
	m.lock.Lock()
	m.actions = append(m.actions, "AfterFilePut")
	m.lock.Unlock()
}
func (m *mockNotifier) AfterFileDeleted(conn *Conn, dstPath string, err error) {
	m.lock.Lock()
	m.actions = append(m.actions, "AfterFileDeleted")
	m.lock.Unlock()
}
func (m *mockNotifier) AfterCurDirChanged(conn *Conn, oldCurDir, newCurDir string, err error) {
	m.lock.Lock()
	m.actions = append(m.actions, "AfterCurDirChanged")
	m.lock.Unlock()
}
func (m *mockNotifier) AfterDirCreated(conn *Conn, dstPath string, err error) {
	m.lock.Lock()
	m.actions = append(m.actions, "AfterDirCreated")
	m.lock.Unlock()
}
func (m *mockNotifier) AfterDirDeleted(conn *Conn, dstPath string, err error) {
	m.lock.Lock()
	m.actions = append(m.actions, "AfterDirDeleted")
	m.lock.Unlock()
}
func (m *mockNotifier) AfterFileDownloaded(conn *Conn, dstPath string, size int64, err error) {
	m.lock.Lock()
	m.actions = append(m.actions, "AfterFileDownloaded")
	m.lock.Unlock()
}

func assetMockNotifier(t *testing.T, mock *mockNotifier, lastActions []string) {
	if len(lastActions) == 0 {
		return
	}
	mock.lock.Lock()
	assert.EqualValues(t, lastActions, mock.actions[len(mock.actions)-len(lastActions):])
	mock.lock.Unlock()
}

func TestNotification(t *testing.T) {
	os.MkdirAll("./testdata", os.ModePerm)

	var perm = NewSimplePerm("test", "test")
	opt := &ServerOpts{
		Name: "test ftpd",
		Factory: &FileDriverFactory{
			RootPath: "./testdata",
			Perm:     perm,
		},
		Port: 2121,
		Auth: &SimpleAuth{
			Name:     "admin",
			Password: "admin",
		},
		Logger: new(DiscardLogger),
	}

	mock := &mockNotifier{}

	runServer(t, opt, notifierList{mock}, func() {
		// Give server 0.5 seconds to get to the listening state
		timeout := time.NewTimer(time.Millisecond * 500)

		for {
			f, err := ftp.Connect("localhost:2121")
			if err != nil && len(timeout.C) == 0 { // Retry errors
				continue
			}
			assert.NoError(t, err)

			assert.NoError(t, f.Login("admin", "admin"))
			assetMockNotifier(t, mock, []string{"BeforeLoginUser", "AfterUserLogin"})

			assert.Error(t, f.Login("admin", "1111"))
			assetMockNotifier(t, mock, []string{"BeforeLoginUser", "AfterUserLogin"})

			var content = `test`
			assert.NoError(t, f.Stor("server_test.go", strings.NewReader(content)))
			assetMockNotifier(t, mock, []string{"BeforePutFile", "AfterFilePut"})

			r, err := f.RetrFrom("/server_test.go", 2)
			assert.NoError(t, err)

			buf, err := ioutil.ReadAll(r)
			r.Close()
			assert.NoError(t, err)
			assert.EqualValues(t, "st", string(buf))
			assetMockNotifier(t, mock, []string{"BeforeDownloadFile", "AfterFileDownloaded"})

			err = f.Rename("/server_test.go", "/test.go")
			assert.NoError(t, err)

			err = f.MakeDir("/src")
			assetMockNotifier(t, mock, []string{"BeforeCreateDir", "AfterDirCreated"})

			err = f.Delete("/test.go")
			assetMockNotifier(t, mock, []string{"BeforeDeleteFile", "AfterFileDeleted"})

			err = f.ChangeDir("/src")
			assetMockNotifier(t, mock, []string{"BeforeChangeCurDir", "AfterCurDirChanged"})

			err = f.RemoveDir("/src")
			assetMockNotifier(t, mock, []string{"BeforeDeleteDir", "AfterDirDeleted"})

			err = f.Quit()
			assert.NoError(t, err)

			break
		}
	})
}
