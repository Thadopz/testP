package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestPrometheusRecorderExposesNodeAndShardMetrics(t *testing.T) {
	recorder := NewPrometheusRecorder(prometheus.NewRegistry())

	recorder.SetNodeOwnedShards(10, 2)
	recorder.SetNodeSubmitted(10, 11)
	recorder.SetNodeMatched(10, 7)
	recorder.SetNodeMissed(10, 4)
	recorder.SetNodeOnlineRiders(10, 99)
	recorder.SetShardCheckpointOffset(10, 1, 100)
	recorder.SetShardEventLogOffset(10, 1, 120)
	recorder.SetShardLag(10, 1, 20)
	recorder.SetShardEpoch(10, 1, 3)
	recorder.IncEventApply(10, 1, "order_created")
	recorder.IncEventApplyError(10, 1, "order_created")
	recorder.IncFencingReject(10, 1)
	recorder.SetControllerLeader("controller-1", true)
	recorder.IncControllerSweep("controller-1")
	recorder.IncControllerSweepError("controller-1", "election")
	recorder.IncFailover("controller-1", 2)
	recorder.SetAliveNodes("controller-1", 3)
	recorder.SetOwnedShards("controller-1", 64)
	recorder.SetShardsWithoutOwner("controller-1", 0)
	recorder.IncProducerEvent("order_created", 1)
	recorder.IncProducerError("append")

	request := httptest.NewRequest("GET", "/metrics", nil)
	response := httptest.NewRecorder()
	recorder.Handler().ServeHTTP(response, request)

	bodyBytes, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	body := string(bodyBytes)

	assertMetricLine(t, body, `testp_node_owned_shards{node_id="10"} 2`)
	assertMetricLine(t, body, `testp_node_submitted_total{node_id="10"} 11`)
	assertMetricLine(t, body, `testp_node_matched_total{node_id="10"} 7`)
	assertMetricLine(t, body, `testp_node_missed_total{node_id="10"} 4`)
	assertMetricLine(t, body, `testp_node_online_riders{node_id="10"} 99`)
	assertMetricLine(t, body, `testp_shard_checkpoint_offset{node_id="10",shard_id="1"} 100`)
	assertMetricLine(t, body, `testp_shard_eventlog_offset{node_id="10",shard_id="1"} 120`)
	assertMetricLine(t, body, `testp_shard_lag{node_id="10",shard_id="1"} 20`)
	assertMetricLine(t, body, `testp_shard_epoch{node_id="10",shard_id="1"} 3`)
	assertMetricLine(t, body, `testp_event_apply_total{event_type="order_created",node_id="10",shard_id="1"} 1`)
	assertMetricLine(t, body, `testp_event_apply_errors_total{event_type="order_created",node_id="10",shard_id="1"} 1`)
	assertMetricLine(t, body, `testp_fencing_reject_total{node_id="10",shard_id="1"} 1`)
	assertMetricLine(t, body, `testp_controller_leader{controller_id="controller-1"} 1`)
	assertMetricLine(t, body, `testp_controller_sweep_total{controller_id="controller-1"} 1`)
	assertMetricLine(t, body, `testp_controller_sweep_errors_total{controller_id="controller-1",reason="election"} 1`)
	assertMetricLine(t, body, `testp_failover_total{controller_id="controller-1",dead_node_id="2"} 1`)
	assertMetricLine(t, body, `testp_alive_nodes{controller_id="controller-1"} 3`)
	assertMetricLine(t, body, `testp_owned_shards_total{controller_id="controller-1"} 64`)
	assertMetricLine(t, body, `testp_shards_without_owner{controller_id="controller-1"} 0`)
	assertMetricLine(t, body, `testp_producer_events_total{event_type="order_created",shard_id="1"} 1`)
	assertMetricLine(t, body, `testp_producer_errors_total{reason="append"} 1`)
}

func assertMetricLine(t *testing.T, body string, expected string) {
	t.Helper()

	if !strings.Contains(body, expected) {
		t.Fatalf("metric line %q not found in:\n%s", expected, body)
	}
}
