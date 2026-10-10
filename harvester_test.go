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
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// newTestHarvester 在一个临时目录中构造 Harvester：日志文件预置内容，元数据路径独立。
func newTestHarvester(t *testing.T) (Harvester, string) {
	t.Helper()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte("line-1\nline-2\nline-3\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	metaPath := filepath.Join(dir, "meta.json")
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	h, err := NewHarvester(Config{
		Path:     logPath,
		MetaPath: metaPath,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h, logPath
}

// runWithTimeout 在超时保护下执行 fn，避免回归导致的死锁把测试永久挂住。
func runWithTimeout(t *testing.T, d time.Duration, fn func() error) error {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- fn() }()

	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("operation did not finish within %s (possible deadlock)", d)
		return nil
	}
}

// waitUntil 轮询等待条件成立，超时则失败。
func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not satisfied within %s", d)
}

// waitGoroutineAtLeast 等待至少 want 个 goroutine 存在。
func waitGoroutineAtLeast(t *testing.T, want int, d time.Duration) {
	t.Helper()

	waitUntil(t, d, func() bool { return runtime.NumGoroutine() >= want })
}

// waitGoroutineAtMost 等待 goroutine 数量回落到 want 以内，用于断言后台 goroutine 已回收。
func waitGoroutineAtMost(t *testing.T, want int, d time.Duration) {
	t.Helper()

	waitUntil(t, d, func() bool { return runtime.NumGoroutine() <= want })
}

// countingSink 统计收到的消息数量。
type countingSink struct {
	n int32
}

func (s *countingSink) OnMessage(string) { atomic.AddInt32(&s.n, 1) }

// TestHarvesterCloseStopsGoroutines 验证：Run 后两个后台 goroutine 已启动，
// Close 返回前它们全部退出，且 Go 运行时观测到 goroutine 数量回落。
func TestHarvesterCloseStopsGoroutines(t *testing.T) {
	base := runtime.NumGoroutine()

	h, _ := newTestHarvester(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.Run(ctx)

	// 修复前：Close 只关闭 reader，两个后台 goroutine 会继续泄漏。
	waitGoroutineAtLeast(t, base+2, 5*time.Second)

	if err := runWithTimeout(t, 5*time.Second, h.Close); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	// Close 内部已 wg.Wait，返回即意味着两个后台 goroutine 均已结束。
	waitGoroutineAtMost(t, base, 5*time.Second)
}

// TestHarvesterCloseWithoutRun 验证：从未调用 Run（curReader 为 nil）时 Close 不 panic、正常返回。
func TestHarvesterCloseWithoutRun(t *testing.T) {
	h, _ := newTestHarvester(t)

	// 修复前：curReader.Load() 为 nil，类型断言 `.(Reader)` 会 panic。
	if err := runWithTimeout(t, 5*time.Second, h.Close); err != nil {
		t.Fatalf("Close without Run returned error: %v", err)
	}

	// 再次调用仍然安全
	if err := h.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
}

// TestHarvesterCloseIdempotentConcurrent 验证：并发与连续重复 Close 幂等、无死锁、无 panic。
func TestHarvesterCloseIdempotentConcurrent(t *testing.T) {
	base := runtime.NumGoroutine()

	h, _ := newTestHarvester(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.Run(ctx)
	waitGoroutineAtLeast(t, base+2, 5*time.Second)

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = h.Close()
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Close deadlocked")
	}

	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("concurrent Close #%d returned error: %v", i, errs[i])
		}
	}

	// 顺序重复调用同样安全
	for i := 0; i < 3; i++ {
		if err := h.Close(); err != nil {
			t.Fatalf("sequential Close #%d returned error: %v", i, err)
		}
	}

	waitGoroutineAtMost(t, base, 5*time.Second)
}

// TestHarvesterCloseBeforeAnyFile 验证：当日志目录暂无匹配文件时，
// 两个后台 goroutine 分别阻塞在“等待文件”与条件变量上，Close 仍能唤醒并安全返回。
func TestHarvesterCloseBeforeAnyFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log") // 刻意不创建，setWaitDealFiles 将进入等待
	metaPath := filepath.Join(dir, "meta.json")

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	h, err := NewHarvester(Config{Path: logPath, MetaPath: metaPath, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}

	base := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.Run(ctx)
	waitGoroutineAtLeast(t, base+2, 5*time.Second)

	// 修复前：goroutine1 卡在 setWaitDealFiles 的 sleep 循环、goroutine2 卡在 Cond.Wait，
	// 二者都不观察 ctx，Close 无法使它们退出。
	if err := runWithTimeout(t, 5*time.Second, h.Close); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	waitGoroutineAtMost(t, base, 5*time.Second)
}

// TestHarvesterCloseStopsReading 验证：Close 返回后不再有后台读取访问 reader，
// 追加日志也不会再产生 Sink 回调。
func TestHarvesterCloseStopsReading(t *testing.T) {
	h, logPath := newTestHarvester(t)

	sink := &countingSink{}
	h.RegisterSink(sink)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.Run(ctx)

	// 先确认后台 goroutine 确实在读取日志
	waitUntil(t, 5*time.Second, func() bool { return atomic.LoadInt32(&sink.n) > 0 })

	if err := runWithTimeout(t, 5*time.Second, h.Close); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	before := atomic.LoadInt32(&sink.n)

	// Close 之后追加日志：reader 已关闭、goroutine 已退出，不应再产生回调
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("after-close\n"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)

	if got := atomic.LoadInt32(&sink.n); got != before {
		t.Fatalf("reader consumed data after Close: before=%d after=%d", before, got)
	}
}
