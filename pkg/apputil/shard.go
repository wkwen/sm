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
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ShardSpec struct {
	// 存储下，方便开发
	Service string `json:"service"`

	// 任务内容
	Task string `json:"task"`

	UpdateTime int64 `json:"updateTime"`

	// 通过api可以给shard主动分配到某个container
	ManualContainerId string `json:"manualContainerId"`

	// Group 同一个service需要区分不同种类的shard，
	// 这些shard之间不相关的balance到现有container上
	Group string `json:"group"`
}

func (ss *ShardSpec) String() string {
	b, _ := json.Marshal(ss)
	return string(b)
}

func (ss *ShardSpec) Validate() error {
	if ss.Service == "" {
		return errors.New("Empty service")
	}
	if ss.Task == "" {
		return errors.New("Empty task")
	}
	if ss.UpdateTime <= 0 {
		return errors.New("Err updateTime")
	}
	return nil
}

type ShardHbData struct {
	Load        string `json:"load"`
	ContainerId string `json:"containerId"`
}

func (s *ShardHbData) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

type ShardInterface interface {
	Add(ctx context.Context, id string, spec *ShardSpec) error
	Drop(ctx context.Context, id string) error
	Load(ctx context.Context, id string) (string, error)
	Shards(ctx context.Context) ([]string, error)
}

type ShardOpMessage struct {
	Id      string `json:"id"`
	Type    OpType `json:"type"`
	TraceId string `json:"traceId"`
}

type OpType int

const (
	// OpTypeAdd 0保留，因为go默认int的值是0，防止无意识的错误
	// 参考：https://github.com/etcd-io/etcd/blob/main/client/v3/op.go#L22
	OpTypeAdd OpType = iota + 1
	OpTypeDrop
)

type ShardOpReceiver interface {
	AddShard(c *gin.Context)
	DropShard(c *gin.Context)
}

// ShardServer 直接帮助接入方把服务器端启动好，引入gin框架，和sarama sdk的接入方式相似，提供消息的chan或者callback func给到接入app的业务逻辑
type ShardServer struct {
	stopper  *GoroutineStopper
	taskNode string

	// 在Close方法中需要能被close掉
	srv *http.Server

	donec chan struct{}

	ctx       context.Context
	impl      ShardInterface
	container *Container
	lg        *zap.Logger
}

type shardServerOptions struct {
	addr            string
	routeAndHandler map[string]func(c *gin.Context)

	// FIXME 和ShardServer重复
	ctx       context.Context
	impl      ShardInterface
	container *Container
	lg        *zap.Logger
	sor       ShardOpReceiver

	// 传入 router 允许shard被集成，降低shard接入对app造成的影响。
	// 例如：现有的web项目使用gin，sm把server启动拿过来也不合适。
	router *gin.Engine

	// etcdPrefix 作为sharded application的数据存储prefix，能通过acl做限制
	// TODO 配合 etcdPrefix 需要有用户名和密码的字段
	etcdPrefix string
}

type ShardServerOption func(options *shardServerOptions)

func ShardServerWithAddr(v string) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.addr = v
	}
}

func ShardServerWithContainer(v *Container) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.container = v
	}
}

func ShardServerWithShardImplementation(v ShardInterface) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.impl = v
	}
}

func ShardServerWithContext(v context.Context) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.ctx = v
	}
}

func ShardServerWithApiHandler(v map[string]func(c *gin.Context)) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.routeAndHandler = v
	}
}

func ShardServerWithLogger(v *zap.Logger) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.lg = v
	}
}

func ShardServerWithShardOpReceiver(v ShardOpReceiver) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.sor = v
	}
}

func ShardServerWithRouter(v *gin.Engine) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.router = v
	}
}

func ShardServerWithEtcdPrefix(v string) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.etcdPrefix = v
	}
}

func NewShardServer(opts ...ShardServerOption) (*ShardServer, error) {
	ops := &shardServerOptions{}
	for _, opt := range opts {
		opt(ops)
	}

	// addr 和 router 二选一，否则啥也不用干了
	if ops.addr == "" && ops.router == nil {
		return nil, errors.New("addr err")
	}
	if ops.container == nil {
		return nil, errors.New("container err")
	}
	if ops.lg == nil {
		return nil, errors.New("lg err")
	}
	if ops.impl == nil {
		return nil, errors.New("impl err")
	}
	if ops.ctx == nil {
		return nil, errors.New("ctx err")
	}

	// FIXME 直接刚常量有点粗糙，暂时没有更好的方案
	InitEtcdPrefix(ops.etcdPrefix)

	ss := ShardServer{
		stopper:  &GoroutineStopper{},
		taskNode: EtcdPathAppShardTask(ops.container.Service()),

		impl:      ops.impl,
		container: ops.container,
		lg:        ops.lg,
		ctx:       ops.ctx,
		donec:     make(chan struct{}),
	}

	// 上传shard的load，load是从接入服务拿到
	ss.stopper.Wrap(func(ctx context.Context) {
		TickerLoop(
			ctx, ops.lg, 3*time.Second, fmt.Sprintf("shard server stop upload load"),
			func(ctx context.Context) error {
				shards, err := ss.impl.Shards(ctx)
				if err != nil {
					return errors.Wrap(err, "")
				}

				for _, id := range shards {
					load, err := ss.impl.Load(ctx, id)
					if err != nil {
						return errors.Wrap(err, "")
					}

					hb := ShardHbData{
						Load:        load,
						ContainerId: ss.container.Id(),
					}

					key := EtcdPathAppShardHbId(ss.container.Service(), id)
					if _, err := ss.container.Client.Put(ctx, key, hb.String(), clientv3.WithLease(ss.container.Session.Lease())); err != nil {
						ops.lg.Error("failed to shard load",
							zap.Error(err),
							zap.String("key", key),
							zap.Reflect("hb", hb),
						)
						return errors.Wrap(err, "")
					}
					ops.lg.Debug("shard heartbeat", zap.String("hbNode", key))
				}
				return nil
			},
		)
	})

	router := ops.router
	if ops.router == nil {
		router = gin.Default()
		if ops.routeAndHandler != nil {
			for route, handler := range ops.routeAndHandler {
				router.Any(route, handler)
			}
		}
	}

	var receiver ShardOpReceiver
	if ops.sor != nil {
		receiver = ops.sor
	} else {
		receiver = &ss
	}
	ssg := router.Group("/sm/admin")
	{
		ssg.POST("/add-shard", receiver.AddShard)
		ssg.POST("/drop-shard", receiver.DropShard)
	}

	// router 为空，就帮助启动webserver，相当于app自己选择被集成，例如sm自己
	if ops.router == nil {
		// https://learnku.com/docs/gin-gonic/2019/examples-graceful-restart-or-stop/6173
		srv := &http.Server{
			Addr:    ops.addr,
			Handler: router,
		}
		ss.srv = srv

		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				ops.lg.Panic("failed to listen",
					zap.Error(err),
					zap.String("addr", ops.addr),
				)
			}
		}()
	}

	go func() {
		defer ss.Close()

		// shardserver使用container与etcd建立的session，回收心跳节点
		select {
		case <-ss.container.Session.Done():
			ss.lg.Info("session closed", zap.String("service", ss.container.Service()))
		case <-ops.ctx.Done():
			ss.lg.Info("context done", zap.String("service", ss.container.Service()))
		}
	}()

	return &ss, nil
}

func (ss *ShardServer) Close() {
	defer close(ss.donec)

	if ss.srv != nil {
		if err := ss.srv.Shutdown(ss.ctx); err != nil {
			ss.lg.Error("failed to shutdown http srv",
				zap.Error(err),
				zap.String("service", ss.container.Service()),
			)

		} else {
			ss.lg.Info("shutdown http srv success", zap.String("service", ss.container.Service()))
		}
	}
	if ss.stopper != nil {
		ss.stopper.Close()
	}

	ss.lg.Info("shard server closed", zap.String("service", ss.container.Service()))
}

func (ss *ShardServer) Done() <-chan struct{} {
	return ss.donec
}

func (ss *ShardServer) Container() *Container {
	return ss.container
}

func (ss *ShardServer) AddShard(c *gin.Context) {
	var req ShardOpMessage
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.Type = OpTypeAdd

	shardNode := EtcdPathAppShardId(ss.container.Service(), req.Id)
	sresp, err := ss.container.Client.GetKV(context.TODO(), shardNode, nil)
	if err != nil {
		ss.lg.Error("GetKV err",
			zap.Error(err),
			zap.String("shardNode", shardNode),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var spec ShardSpec
	if err := json.Unmarshal(sresp.Kvs[0].Value, &spec); err != nil {
		ss.lg.Error("Unmarshal err",
			zap.Error(err),
			zap.ByteString("value", sresp.Kvs[0].Value),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// shard属性校验
	if err := spec.Validate(); err != nil {
		ss.lg.Error("shard spec etcd value err",
			zap.Error(err),
			zap.String("value", string(sresp.Kvs[0].Value)),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// container校验
	if spec.ManualContainerId != "" && spec.ManualContainerId != ss.container.Id() {
		ss.lg.Error("unexpected container for shard",
			zap.String("service", ss.container.Service()),
			zap.String("actual", ss.container.Id()),
			zap.String("expect", spec.ManualContainerId),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": "unexpected container"})
		return
	}

	if err := ss.impl.Add(context.TODO(), req.Id, &spec); err != nil {
		ss.lg.Error("Add err",
			zap.Error(err),
			zap.String("id", req.Id),
			zap.Reflect("spec", spec),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ss.lg.Info("add shard request success", zap.Reflect("request", req))

	c.JSON(http.StatusOK, gin.H{})
}

func (ss *ShardServer) DropShard(c *gin.Context) {
	var req ShardOpMessage
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.Type = OpTypeDrop

	if err := ss.impl.Drop(context.TODO(), req.Id); err != nil {
		ss.lg.Error("Drop err",
			zap.Error(err),
			zap.String("id", req.Id),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ss.lg.Info("drop shard success", zap.Reflect("req", req))
	c.JSON(http.StatusOK, gin.H{})
}
