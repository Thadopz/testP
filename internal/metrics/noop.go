package metrics

type NoopRecorder struct{}

func (NoopRecorder) SetNodeOwnedShards(nodeID int, count int) {}

func (NoopRecorder) SetNodeSubmitted(nodeID int, value int64) {}

func (NoopRecorder) SetNodeMatched(nodeID int, value int64) {}

func (NoopRecorder) SetNodeMissed(nodeID int, value int64) {}

func (NoopRecorder) SetNodeOnlineRiders(nodeID int, value int) {}

func (NoopRecorder) SetShardCheckpointOffset(nodeID int, shardID int, offset int64) {}

func (NoopRecorder) SetShardEventLogOffset(nodeID int, shardID int, offset int64) {}

func (NoopRecorder) SetShardLag(nodeID int, shardID int, lag int64) {}

func (NoopRecorder) SetShardEpoch(nodeID int, shardID int, epoch int64) {}

func (NoopRecorder) IncEventApply(nodeID int, shardID int, eventType string) {}

func (NoopRecorder) IncEventApplyError(nodeID int, shardID int, eventType string) {}

func (NoopRecorder) IncFencingReject(nodeID int, shardID int) {}

func (NoopRecorder) SetControllerLeader(controllerID string, leader bool) {}

func (NoopRecorder) IncControllerSweep(controllerID string) {}

func (NoopRecorder) IncControllerSweepError(controllerID string, reason string) {}

func (NoopRecorder) IncFailover(controllerID string, deadNodeID int) {}

func (NoopRecorder) SetAliveNodes(controllerID string, count int) {}

func (NoopRecorder) SetOwnedShards(controllerID string, count int) {}

func (NoopRecorder) SetShardsWithoutOwner(controllerID string, count int) {}

func (NoopRecorder) IncProducerEvent(eventType string, shardID int) {}

func (NoopRecorder) IncProducerError(reason string) {}
