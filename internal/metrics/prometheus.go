package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type PrometheusRecorder struct {
	registry              *prometheus.Registry
	nodeOwnedShards       *prometheus.GaugeVec
	nodeSubmitted         *prometheus.GaugeVec
	nodeMatched           *prometheus.GaugeVec
	nodeMissed            *prometheus.GaugeVec
	nodeOnlineRiders      *prometheus.GaugeVec
	shardCheckpointOffset *prometheus.GaugeVec
	shardEventLogOffset   *prometheus.GaugeVec
	shardLag              *prometheus.GaugeVec
	shardEpoch            *prometheus.GaugeVec
	eventApplies          *prometheus.CounterVec
	eventApplyErrors      *prometheus.CounterVec
	fencingRejects        *prometheus.CounterVec
	controllerLeader      *prometheus.GaugeVec
	controllerSweeps      *prometheus.CounterVec
	controllerSweepErrors *prometheus.CounterVec
	failovers             *prometheus.CounterVec
	aliveNodes            *prometheus.GaugeVec
	ownedShards           *prometheus.GaugeVec
	shardsWithoutOwner    *prometheus.GaugeVec
	producerEvents        *prometheus.CounterVec
	producerErrors        *prometheus.CounterVec
}

func NewPrometheusRecorder(registry *prometheus.Registry) *PrometheusRecorder {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}

	recorder := &PrometheusRecorder{
		registry: registry,
		nodeOwnedShards: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "node_owned_shards",
			Help:      "Number of shards currently owned by a node.",
		}, []string{"node_id"}),
		nodeSubmitted: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "node_submitted_total",
			Help:      "Total submitted orders observed by a node.",
		}, []string{"node_id"}),
		nodeMatched: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "node_matched_total",
			Help:      "Total matched orders observed by a node.",
		}, []string{"node_id"}),
		nodeMissed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "node_missed_total",
			Help:      "Total missed orders observed by a node.",
		}, []string{"node_id"}),
		nodeOnlineRiders: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "node_online_riders",
			Help:      "Number of riders currently online in a node engine.",
		}, []string{"node_id"}),
		shardCheckpointOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "shard_checkpoint_offset",
			Help:      "Last saved checkpoint offset for a shard.",
		}, []string{"node_id", "shard_id"}),
		shardEventLogOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "shard_eventlog_offset",
			Help:      "Current end offset in the event log for a shard.",
		}, []string{"node_id", "shard_id"}),
		shardLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "shard_lag",
			Help:      "Event log end offset minus checkpoint offset for a shard.",
		}, []string{"node_id", "shard_id"}),
		shardEpoch: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "shard_epoch",
			Help:      "Current ownership epoch for a shard.",
		}, []string{"node_id", "shard_id"}),
		eventApplies: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "testp",
			Name:      "event_apply_total",
			Help:      "Total successfully applied events.",
		}, []string{"node_id", "shard_id", "event_type"}),
		eventApplyErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "testp",
			Name:      "event_apply_errors_total",
			Help:      "Total event apply errors.",
		}, []string{"node_id", "shard_id", "event_type"}),
		fencingRejects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "testp",
			Name:      "fencing_reject_total",
			Help:      "Total events rejected because ownership fencing failed.",
		}, []string{"node_id", "shard_id"}),
		controllerLeader: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "controller_leader",
			Help:      "Whether a controller is currently leader. 1 means leader, 0 means follower.",
		}, []string{"controller_id"}),
		controllerSweeps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "testp",
			Name:      "controller_sweep_total",
			Help:      "Total controller sweep attempts.",
		}, []string{"controller_id"}),
		controllerSweepErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "testp",
			Name:      "controller_sweep_errors_total",
			Help:      "Total controller sweep errors.",
		}, []string{"controller_id", "reason"}),
		failovers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "testp",
			Name:      "failover_total",
			Help:      "Total failover operations by dead node.",
		}, []string{"controller_id", "dead_node_id"}),
		aliveNodes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "alive_nodes",
			Help:      "Number of alive nodes observed by a controller.",
		}, []string{"controller_id"}),
		ownedShards: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "owned_shards_total",
			Help:      "Number of shards with an owner observed by a controller.",
		}, []string{"controller_id"}),
		shardsWithoutOwner: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "testp",
			Name:      "shards_without_owner",
			Help:      "Number of shards without an owner observed by a controller.",
		}, []string{"controller_id"}),
		producerEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "testp",
			Name:      "producer_events_total",
			Help:      "Total events written by producer.",
		}, []string{"event_type", "shard_id"}),
		producerErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "testp",
			Name:      "producer_errors_total",
			Help:      "Total producer errors.",
		}, []string{"reason"}),
	}

	registry.MustRegister(
		recorder.nodeOwnedShards,
		recorder.nodeSubmitted,
		recorder.nodeMatched,
		recorder.nodeMissed,
		recorder.nodeOnlineRiders,
		recorder.shardCheckpointOffset,
		recorder.shardEventLogOffset,
		recorder.shardLag,
		recorder.shardEpoch,
		recorder.eventApplies,
		recorder.eventApplyErrors,
		recorder.fencingRejects,
		recorder.controllerLeader,
		recorder.controllerSweeps,
		recorder.controllerSweepErrors,
		recorder.failovers,
		recorder.aliveNodes,
		recorder.ownedShards,
		recorder.shardsWithoutOwner,
		recorder.producerEvents,
		recorder.producerErrors,
	)

	return recorder
}

func (r *PrometheusRecorder) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}

func (r *PrometheusRecorder) SetNodeOwnedShards(nodeID int, count int) {
	r.nodeOwnedShards.WithLabelValues(intLabel(nodeID)).Set(float64(count))
}

func (r *PrometheusRecorder) SetNodeSubmitted(nodeID int, value int64) {
	r.nodeSubmitted.WithLabelValues(intLabel(nodeID)).Set(float64(value))
}

func (r *PrometheusRecorder) SetNodeMatched(nodeID int, value int64) {
	r.nodeMatched.WithLabelValues(intLabel(nodeID)).Set(float64(value))
}

func (r *PrometheusRecorder) SetNodeMissed(nodeID int, value int64) {
	r.nodeMissed.WithLabelValues(intLabel(nodeID)).Set(float64(value))
}

func (r *PrometheusRecorder) SetNodeOnlineRiders(nodeID int, value int) {
	r.nodeOnlineRiders.WithLabelValues(intLabel(nodeID)).Set(float64(value))
}

func (r *PrometheusRecorder) SetShardCheckpointOffset(nodeID int, shardID int, offset int64) {
	r.shardCheckpointOffset.WithLabelValues(intLabel(nodeID), intLabel(shardID)).Set(float64(offset))
}

func (r *PrometheusRecorder) SetShardEventLogOffset(nodeID int, shardID int, offset int64) {
	r.shardEventLogOffset.WithLabelValues(intLabel(nodeID), intLabel(shardID)).Set(float64(offset))
}

func (r *PrometheusRecorder) SetShardLag(nodeID int, shardID int, lag int64) {
	r.shardLag.WithLabelValues(intLabel(nodeID), intLabel(shardID)).Set(float64(lag))
}

func (r *PrometheusRecorder) SetShardEpoch(nodeID int, shardID int, epoch int64) {
	r.shardEpoch.WithLabelValues(intLabel(nodeID), intLabel(shardID)).Set(float64(epoch))
}

func (r *PrometheusRecorder) IncEventApply(nodeID int, shardID int, eventType string) {
	r.eventApplies.WithLabelValues(intLabel(nodeID), intLabel(shardID), eventType).Inc()
}

func (r *PrometheusRecorder) IncEventApplyError(nodeID int, shardID int, eventType string) {
	r.eventApplyErrors.WithLabelValues(intLabel(nodeID), intLabel(shardID), eventType).Inc()
}

func (r *PrometheusRecorder) IncFencingReject(nodeID int, shardID int) {
	r.fencingRejects.WithLabelValues(intLabel(nodeID), intLabel(shardID)).Inc()
}

func (r *PrometheusRecorder) SetControllerLeader(controllerID string, leader bool) {
	value := 0.0
	if leader {
		value = 1
	}
	r.controllerLeader.WithLabelValues(controllerID).Set(value)
}

func (r *PrometheusRecorder) IncControllerSweep(controllerID string) {
	r.controllerSweeps.WithLabelValues(controllerID).Inc()
}

func (r *PrometheusRecorder) IncControllerSweepError(controllerID string, reason string) {
	r.controllerSweepErrors.WithLabelValues(controllerID, reason).Inc()
}

func (r *PrometheusRecorder) IncFailover(controllerID string, deadNodeID int) {
	r.failovers.WithLabelValues(controllerID, intLabel(deadNodeID)).Inc()
}

func (r *PrometheusRecorder) SetAliveNodes(controllerID string, count int) {
	r.aliveNodes.WithLabelValues(controllerID).Set(float64(count))
}

func (r *PrometheusRecorder) SetOwnedShards(controllerID string, count int) {
	r.ownedShards.WithLabelValues(controllerID).Set(float64(count))
}

func (r *PrometheusRecorder) SetShardsWithoutOwner(controllerID string, count int) {
	r.shardsWithoutOwner.WithLabelValues(controllerID).Set(float64(count))
}

func (r *PrometheusRecorder) IncProducerEvent(eventType string, shardID int) {
	r.producerEvents.WithLabelValues(eventType, intLabel(shardID)).Inc()
}

func (r *PrometheusRecorder) IncProducerError(reason string) {
	r.producerErrors.WithLabelValues(reason).Inc()
}

func intLabel(value int) string {
	return strconv.Itoa(value)
}
