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

package smserver

import (
	"fmt"
	"hash/crc32"
	"sort"
	"testing"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

func Test_shardTask(t *testing.T) {
	task := &shardTask{GovernedService: ""}
	if task.Validate() {
		t.Error("Validate should be false")
		t.SkipNow()
	}
}

func TestShard(t *testing.T) {
	suite.Run(t, new(ShardTestSuite))
}

type ShardTestSuite struct {
	suite.Suite

	shard *smShard
}

func (suite *ShardTestSuite) SetupTest() {
	lg, _ := zap.NewDevelopment()

	shard := &smShard{
		lg: lg,

		service: mock.Anything,
	}
	suite.shard = shard
}

func (suite *ShardTestSuite) TestChanged() {
	var tests = []struct {
		a      []string
		b      []string
		expect bool
	}{
		{
			a:      []string{},
			b:      []string{},
			expect: false,
		},
		{
			a:      []string{"1"},
			b:      []string{"1"},
			expect: false,
		},
		{
			a:      []string{"1"},
			b:      []string{},
			expect: true,
		},
		{
			a:      []string{"1", "2"},
			b:      []string{"2", "1"},
			expect: false,
		},
		{
			a:      []string{"1", "2"},
			b:      []string{"1", "2", "3"},
			expect: true,
		},
	}
	for _, tt := range tests {
		r := suite.shard.changed(tt.a, tt.b)
		assert.Equal(suite.T(), r, tt.expect)
	}
}

func (suite *ShardTestSuite) TestRB() {
	shardIdAndShardSpec := make(map[string]*apputil.ShardSpec)
	shardIdAndShardSpec["s1"] = &apputil.ShardSpec{}
	shardIdAndShardSpec["s2"] = &apputil.ShardSpec{}
	shardIdAndShardSpec["s3"] = &apputil.ShardSpec{}
	var tests = []struct {
		fixShardIdAndManualContainerId ArmorMap
		hbContainerIdAndAny            ArmorMap
		hbShardIdAndContainerId        ArmorMap
		containerIdAndWorkerGroups     map[string]map[string]struct{}
		expect                         moveActionList
	}{
		// container新增
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
				"s3": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c1",
			},
			containerIdAndWorkerGroups: make(map[string]map[string]struct{}),
			expect: moveActionList{
				&moveAction{Service: suite.shard.service, ShardId: "s3", AddEndpoint: "c2", Spec: shardIdAndShardSpec["s3"]},
			},
		},

		// container新增
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c1",
			},
			containerIdAndWorkerGroups: make(map[string]map[string]struct{}),
			expect: moveActionList{
				&moveAction{Service: suite.shard.service, ShardId: "s1", DropEndpoint: "c1", AddEndpoint: "c2", Spec: shardIdAndShardSpec["s1"]},
			},
		},

		// container存活，没有shard需要移动
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c2",
			},
			containerIdAndWorkerGroups: make(map[string]map[string]struct{}),
			expect:                     nil,
		},
		// container存活，没有shard需要移动，和顺序无关
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c2",
				"s2": "c1",
			},
			containerIdAndWorkerGroups: make(map[string]map[string]struct{}),
			expect:                     nil,
		},
		// container存活，没有shard需要移动，和顺序无关
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "",
			},
			containerIdAndWorkerGroups: make(map[string]map[string]struct{}),
			expect:                     nil,
		},

		// container不存活，数据不一致不处理
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c2",
			},
			containerIdAndWorkerGroups: make(map[string]map[string]struct{}),
			expect:                     nil,
		},
	}

	for _, tt := range tests {
		r := suite.shard.extractShardMoves(tt.fixShardIdAndManualContainerId, tt.hbContainerIdAndAny, tt.hbShardIdAndContainerId, shardIdAndShardSpec, tt.containerIdAndWorkerGroups)
		assert.Equal(suite.T(), r, tt.expect)
	}
}

func (suite *ShardTestSuite) TestWorkerGroupRB() {
	var (
		equal    = "equal"
		contains = "contains"
	)

	var tests = []struct {
		fixShardIdAndManualContainerId ArmorMap
		hbContainerIdAndAny            ArmorMap
		hbShardIdAndContainerId        ArmorMap
		containerIdAndWorkerGroups     map[string]map[string]struct{}
		shardIdAndShardSpec            map[string]*apputil.ShardSpec
		expect                         moveActionList
		exceptPolicy                   string
	}{
		// 初始配置，开始分配
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
				"s3": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{},
			shardIdAndShardSpec: map[string]*apputil.ShardSpec{
				"s1": {WorkerGroup: "w1"},
				"s2": {WorkerGroup: "w1"},
				"s3": {WorkerGroup: "w2"},
			},
			containerIdAndWorkerGroups: map[string]map[string]struct{}{
				"c1": {"w1": struct{}{}},
				"c2": {"w2": struct{}{}},
			},
			expect: moveActionList{
				&moveAction{Service: suite.shard.service, ShardId: "s1", AddEndpoint: "c1", Spec: &apputil.ShardSpec{WorkerGroup: "w1"},
				},
				&moveAction{Service: suite.shard.service, ShardId: "s2", AddEndpoint: "c1", Spec: &apputil.ShardSpec{WorkerGroup: "w1"},
				},
				&moveAction{Service: suite.shard.service, ShardId: "s3", AddEndpoint: "c2", Spec: &apputil.ShardSpec{WorkerGroup: "w2"},
				},
			},
			exceptPolicy: equal},

		// workerGroup新增
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
				"s3": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c1",
				"s3": "c1",
			},
			shardIdAndShardSpec: map[string]*apputil.ShardSpec{
				"s1": {WorkerGroup: "w1"},
				"s2": {WorkerGroup: "w1"},
				"s3": {WorkerGroup: "w1"},
			},
			containerIdAndWorkerGroups: map[string]map[string]struct{}{
				"c1": {"w1": struct{}{}},
				"c2": {"w1": struct{}{}},
			},
			expect: moveActionList{
				&moveAction{Service: suite.shard.service, ShardId: "s1", DropEndpoint: "c1", AddEndpoint: "c2", Spec: &apputil.ShardSpec{WorkerGroup: "w1"},
				},
				&moveAction{Service: suite.shard.service, ShardId: "s2", DropEndpoint: "c1", AddEndpoint: "c2", Spec: &apputil.ShardSpec{WorkerGroup: "w1"},
				},
				&moveAction{Service: suite.shard.service, ShardId: "s3", DropEndpoint: "c1", AddEndpoint: "c2", Spec: &apputil.ShardSpec{WorkerGroup: "w1"},
				},
			},
			exceptPolicy: contains},

		// shard新增
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
				"s3": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
			},
			shardIdAndShardSpec: map[string]*apputil.ShardSpec{
				"s1": {WorkerGroup: "w1"},
				"s2": {WorkerGroup: "w2"},
				"s3": {WorkerGroup: "w2"},
			},
			containerIdAndWorkerGroups: map[string]map[string]struct{}{
				"c1": {"w1": struct{}{}},
				"c2": {"w2": struct{}{}},
			},
			expect: moveActionList{
				&moveAction{Service: suite.shard.service, ShardId: "s2", AddEndpoint: "c2", Spec: &apputil.ShardSpec{WorkerGroup: "w2"},
				},
				&moveAction{Service: suite.shard.service, ShardId: "s3", AddEndpoint: "c2", Spec: &apputil.ShardSpec{WorkerGroup: "w2"},
				},
			},
			exceptPolicy: equal},

		// container新增
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
				"s3": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
				"c3": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c1",
				"s3": "c2",
			},
			shardIdAndShardSpec: map[string]*apputil.ShardSpec{
				"s1": {WorkerGroup: "w1"},
				"s2": {WorkerGroup: "w1"},
				"s3": {WorkerGroup: "w2"},
			},
			containerIdAndWorkerGroups: map[string]map[string]struct{}{
				"c1": {"w1": struct{}{}},
				"c2": {"w2": struct{}{}},
				"c3": {"w1": struct{}{}},
			},
			expect: moveActionList{
				&moveAction{Service: suite.shard.service, ShardId: "s1", DropEndpoint: "c1", AddEndpoint: "c3", Spec: &apputil.ShardSpec{WorkerGroup: "w1"},
				},
				&moveAction{Service: suite.shard.service, ShardId: "s2", DropEndpoint: "c1", AddEndpoint: "c3", Spec: &apputil.ShardSpec{WorkerGroup: "w1"},
				},
			},
			exceptPolicy: contains},

		// container新增未知的workerGroup,不移动
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
				"s3": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
				"c3": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c1",
				"s3": "c2",
			},
			shardIdAndShardSpec: map[string]*apputil.ShardSpec{
				"s1": {WorkerGroup: "w1"},
				"s2": {WorkerGroup: "w1"},
				"s3": {WorkerGroup: "w2"},
			},
			containerIdAndWorkerGroups: map[string]map[string]struct{}{
				"c1": {"w1": struct{}{}},
				"c2": {"w2": struct{}{}},
				"c3": {"w3": struct{}{}},
			},
			expect:       nil,
			exceptPolicy: equal},

		// 新增未知资源组的shard，不移动
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
				"s3": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c2",
			},
			shardIdAndShardSpec: map[string]*apputil.ShardSpec{
				"s1": {WorkerGroup: "w1"},
				"s2": {WorkerGroup: "w2"},
				"s3": {WorkerGroup: "w3"},
			},
			containerIdAndWorkerGroups: map[string]map[string]struct{}{
				"c1": {"w1": struct{}{}},
				"c2": {"w2": struct{}{}},
			},
			expect:       nil,
			exceptPolicy: equal},

		// shard的资源组不存在，需要drop
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
				"s3": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c1",
				"s3": "c2",
			},
			shardIdAndShardSpec: map[string]*apputil.ShardSpec{
				"s1": {WorkerGroup: "w1"},
				"s2": {WorkerGroup: "w1"},
				"s3": {WorkerGroup: "w3"},
			},
			containerIdAndWorkerGroups: map[string]map[string]struct{}{
				"c1": {"w1": struct{}{}},
				"c2": {"w2": struct{}{}},
			},
			expect: moveActionList{
				&moveAction{Service: suite.shard.service, ShardId: "s3", DropEndpoint: "c2", Spec: &apputil.ShardSpec{WorkerGroup: "w3"},
				},
			},
			exceptPolicy: equal},

		// shard的资源组不包含当前的container,需要move
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
				"s3": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c1",
				"s3": "c1",
			},
			shardIdAndShardSpec: map[string]*apputil.ShardSpec{
				"s1": {WorkerGroup: "w1"},
				"s2": {WorkerGroup: "w1"},
				"s3": {WorkerGroup: "w2"},
			},
			containerIdAndWorkerGroups: map[string]map[string]struct{}{
				"c1": {"w1": struct{}{}},
				"c2": {"w2": struct{}{}},
			},
			expect: moveActionList{
				&moveAction{Service: suite.shard.service, ShardId: "s3", DropEndpoint: "c1", AddEndpoint: "c2", Spec: &apputil.ShardSpec{WorkerGroup: "w2"},
				},
			},
			exceptPolicy: equal},
	}

	for _, tt := range tests {
		r := suite.shard.extractShardMoves(tt.fixShardIdAndManualContainerId, tt.hbContainerIdAndAny, tt.hbShardIdAndContainerId, tt.shardIdAndShardSpec, tt.containerIdAndWorkerGroups)
		sort.Slice(r, func(i, j int) bool {
			return int(crc32.ChecksumIEEE([]byte(r[i].ShardId)))-int(crc32.ChecksumIEEE([]byte(r[j].ShardId))) > 0
		})
		sort.Slice(tt.expect, func(i, j int) bool {
			return int(crc32.ChecksumIEEE([]byte(tt.expect[i].ShardId)))-int(crc32.ChecksumIEEE([]byte(tt.expect[j].ShardId))) > 0
		})
		fmt.Println(r)
		fmt.Println(tt.expect)
		if tt.exceptPolicy == equal {
			assert.Equal(suite.T(), r, tt.expect)
		}
		if tt.exceptPolicy == contains {
			for _, v := range r {
				assert.Contains(suite.T(), tt.expect, v)
			}
		}
	}
}
