// Copyright 2021 The entertainment-venue Authors
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

package apputil

import (
	"context"
	"time"

	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func TickerLoop(ctx context.Context, lg *zap.Logger, duration time.Duration, exitMsg string, fn func(ctx context.Context) error) {
	tickerLoop(ctx, lg, duration, exitMsg, fn, false)
}

func SequenceTickerLoop(ctx context.Context, lg *zap.Logger, duration time.Duration, exitMsg string, fn func(ctx context.Context) error) {
	tickerLoop(ctx, lg, duration, exitMsg, fn, true)
}

func tickerLoop(ctx context.Context, lg *zap.Logger, duration time.Duration, exitMsg string, fn func(ctx context.Context) error, sequence bool) {
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		if sequence {
			// 想要周期又不想要因为fn执行慢，导致多个goroutine并行执行，例如：周期检查rb
			if err := fn(ctx); err != nil {
				lg.Error("TickerLoop err", zap.Error(err))
			}
		} else {
			// 参考PD的lease.go修正ticker的机制
			// 1 先运行一次loop，算是降低下接入应用的RD在心智压力，让程序更可以预估，防止duration需要等待好久的场景
			// 2 利用goroutine隔离不同的fn，尽可能防止单次goroutine STW或者block，但etcd其实健康的情况，没有心跳，但也有goroutine泄漏的风险
			go func() {
				if err := fn(ctx); err != nil {
					lg.Error("TickerLoop err", zap.Error(err))
				}
			}()
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			lg.Info(exitMsg)
			return
		}
	}
}

func WatchLoop(ctx context.Context, lg *zap.Logger, client etcdutil.EtcdWrapper, key string, rev int64, fn func(ctx context.Context, ev *clientv3.Event) error) {
	var (
		startRev int64
		opts     []clientv3.OpOption
		wch      clientv3.WatchChan
	)
	startRev = rev

loop:
	lg.Info(
		"WatchLoop start",
		zap.String("key", key),
		zap.Int64("startRev", startRev),
	)

	opts = append(opts, clientv3.WithPrefix())
	// 允许不关注rev的watch
	if startRev >= 0 {
		opts = append(opts, clientv3.WithRev(startRev))

		// delete事件需要上一个kv
		// https://github.com/etcd-io/etcd/issues/6120
		opts = append(opts, clientv3.WithPrevKV())
	}
	wch = client.Watch(ctx, key, opts...)
	for {
		var wr clientv3.WatchResponse
		select {
		case wr = <-wch:
		case <-ctx.Done():
			lg.Info(
				"WatchLoop exit",
				zap.String("key", key),
				zap.Int64("startRev", startRev),
			)
			return
		}
		if err := wr.Err(); err != nil {
			lg.Error(
				"WatchLoop error",
				zap.String("key", key),
				zap.Int64("startRev", startRev),
				zap.Int64("CompactRevision", wr.CompactRevision),
				zap.Error(err),
			)
			// https://github.com/etcd-io/etcd/issues/8668
			if err == rpctypes.ErrCompacted {
				// 需要重新当前key的最新revision，修正startRev
				resp, err := client.Get(context.Background(), key, clientv3.WithPrefix())
				if err != nil {
					lg.Error(
						"WatchLoop try to get newest revision failed",
						zap.String("key", key),
						zap.Int64("startRev", startRev),
						zap.Error(err),
					)
				} else {
					lg.Info(
						"WatchLoop correct startRev",
						zap.String("key", key),
						zap.Int64("oldStartRev", startRev),
						zap.Int64("newStartRev", resp.Header.Revision+1),
					)
					startRev = resp.Header.Revision + 1
				}
			}
			time.Sleep(300 * time.Millisecond)
			goto loop
		}

		for _, ev := range wr.Events {
			if err := fn(ctx, ev); err != nil {
				lg.Error(
					"WatchLoop error when call fn",
					zap.String("key", key),
					zap.Int64("startRev", startRev),
					zap.Error(err),
				)
			}
		}

		// 发生错误时，从上次的rev开始watch
		startRev = wr.Header.GetRevision() + 1
	}
}
