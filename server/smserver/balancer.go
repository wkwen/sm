package smserver

type balancer struct {
	bcs                        map[string]*balancerContainer
	containerIdAndWorkerGroups map[string]map[string]struct{}
}

type balancerContainer struct {
	// id container标识
	id string

	// workerGroups container属于的资源组
	workerGroups map[string]struct{}

	// shards workerGroup => shard => nothing
	shards map[string]map[string]*balancerShard
}

func (bc *balancerContainer) ContainsWorkerGroup(workerGroup string) bool {
	if workerGroup == "" {
		return true
	}
	if _, ok := bc.workerGroups[workerGroup]; ok {
		return true
	}
	return false
}

type balancerShard struct {
	// id shard标识
	id string

	// isManual 是否是制定container的
	isManual bool

	// workerGroup shard归属的资源组
	workerGroup string
}

func (b *balancer) put(containerId, shardId, workerGroup string, isManual bool) {
	b.addContainer(containerId)
	if b.bcs[containerId].shards[workerGroup] == nil {
		b.bcs[containerId].shards[workerGroup] = make(map[string]*balancerShard)
	}
	b.bcs[containerId].shards[workerGroup][shardId] = &balancerShard{
		id:          shardId,
		isManual:    isManual,
		workerGroup: workerGroup,
	}
}

func (b *balancer) forEach(visitor func(bc *balancerContainer)) {
	for _, bc := range b.bcs {
		visitor(bc)
	}
}

func (b *balancer) addContainer(containerId string) {
	cs := b.bcs[containerId]
	if cs == nil {
		b.bcs[containerId] = &balancerContainer{
			id:           containerId,
			shards:       make(map[string]map[string]*balancerShard),
			workerGroups: make(map[string]struct{}),
		}
		if wgs, ok := b.containerIdAndWorkerGroups[containerId]; ok {
			b.bcs[containerId].workerGroups = wgs
		}
	}
}

// balancerGroup 同一个container支持在shard维度支持分组，分开balance
type balancerGroup struct {
	// fixShardIdAndManualContainerId shard配置
	fixShardIdAndManualContainerId ArmorMap

	// hbShardIdAndContainerId shard心跳
	hbShardIdAndContainerId ArmorMap
}

func newBalanceGroup() *balancerGroup {
	return &balancerGroup{
		fixShardIdAndManualContainerId: make(ArmorMap),
		hbShardIdAndContainerId:        make(ArmorMap),
	}
}

// balancerWorkerGroup shard可以指定分配到哪一个资源组，资源组是由container组成的集合
type balancerWorkerGroup struct {
	// containerCnt container的数量
	containerCnt int

	// shardCnt shard的数量
	shardCnt int

	// maxHold 每个container包含的最大shard数量
	maxHold int
}
