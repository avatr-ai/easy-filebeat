// MIT License

// Copyright (c) 2022 liaochuntao

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package filebeat

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestNewHarvester(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(os.Stderr)
	h, err := NewHarvester(Config{
		Path:     "/data/Github/polaris-server/log/operation/polaris-history.log",
		MetaPath: "./test",
		Logger:   logger,
	})

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	h.Run(ctx)

	<-ctx.Done()
	h.Close()
}

type mockSink struct {
}

// OnMessage
func (s *mockSink) OnMessage(msg string) {
	fmt.Printf("%s\n", msg)
}

// TestCloseWithoutRun verifies that Close() before Run() does not panic.
func TestCloseWithoutRun(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(os.Stderr)

	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta")

	h, err := NewHarvester(Config{
		Path:     filepath.Join(tmpDir, "test.log"),
		MetaPath: metaPath,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Close without Run — should not panic
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestCloseMultipleTimes verifies that calling Close() multiple times
// does not panic or hang.
func TestCloseMultipleTimes(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(os.Stderr)

	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta")
	logFile, err := os.Create(filepath.Join(tmpDir, "test.log"))
	if err != nil {
		t.Fatal(err)
	}
	logFile.Close()

	h, err := NewHarvester(Config{
		Path:     filepath.Join(tmpDir, "test.log"),
		MetaPath: metaPath,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}

	h.Run(context.Background())

	// Multiple closes — should not panic or return error
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestCloseStopsGoroutines verifies that Close() causes the background
// goroutines to exit within a reasonable timeout.
func TestCloseStopsGoroutines(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(os.Stderr)

	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta")
	logFile, err := os.Create(filepath.Join(tmpDir, "test.log"))
	if err != nil {
		t.Fatal(err)
	}
	logFile.Close()

	h, err := NewHarvester(Config{
		Path:     filepath.Join(tmpDir, "test.log"),
		MetaPath: metaPath,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}

	h.Run(context.Background())

	done := make(chan struct{})
	go func() {
		h.Close()
		close(done)
	}()

	select {
	case <-done:
		// Close returned without hanging — goroutines stopped
	case <-time.After(5 * time.Second):
		t.Fatal("Close() timed out — goroutines may not have stopped")
	}
}