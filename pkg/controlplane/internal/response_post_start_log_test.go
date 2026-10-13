package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openai/tunnel-client/pkg/runtimeconfig"
	"github.com/openai/tunnel-client/pkg/tunnelctx"
	"github.com/openai/tunnel-client/pkg/types"
	"github.com/stretchr/testify/require"
)

func startTraceClient(t *testing.T, url string, logs *bytes.Buffer, debug bool) *TunnelServiceClient {
	t.Helper()
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: level}))
	client, err := NewTunnelServiceClient(context.Background(), &runtimeconfig.ControlPlaneConfig{
		BaseURL:  mustParseURL(t, url),
		TunnelID: types.TunnelID("tunnel_post_start_test"),
		APIKey:   "post-start-private-api-key",
	}, nil, logger, &runtimeconfig.LoggingConfig{}, testMeterProvider)
	require.NoError(t, err)
	return client
}

func startTracePost(client *TunnelServiceClient) error {
	ctx := tunnelctx.ContextWithShardToken(context.Background(), "post-start-private-shard")
	ctx = tunnelctx.ContextWithChannel(ctx, types.DefaultChannel)
	response := types.NewTunnelResponse(types.DefaultChannel,
		json.RawMessage("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"value\":\"private-start-body-sentinel\"}}"),
		http.StatusOK, nil)
	_, err := client.PostResponse(ctx, types.RequestID("post-start-test-request"), response)
	return err
}

func startTraceEvents(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range bytes.Split(logs.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var entry map[string]any
		require.NoError(t, json.Unmarshal(line, &entry))
		if entry["msg"] == "control-plane response POST attempt started" {
			out = append(out, entry)
		}
	}
	return out
}

func TestResponsePostStartDebugOnlyAndWireUnchanged(t *testing.T) {
	var lock sync.Mutex
	var bodies [][]byte
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		lock.Lock()
		bodies = append(bodies, append([]byte(nil), raw...))
		lock.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	var debugLogs, infoLogs bytes.Buffer
	require.NoError(t, startTracePost(startTraceClient(t, server.URL, &debugLogs, true)))
	require.NoError(t, startTracePost(startTraceClient(t, server.URL, &infoLogs, false)))
	lock.Lock()
	require.Len(t, bodies, 2)
	require.Equal(t, bodies[0], bodies[1])
	lock.Unlock()
	events := startTraceEvents(t, &debugLogs)
	require.Len(t, events, 1)
	require.EqualValues(t, 1, events[0]["attempt"])
	require.Equal(t, "DEBUG", events[0]["level"])
	require.Empty(t, startTraceEvents(t, &infoLogs))
	require.NotContains(t, debugLogs.String(), "private-start-body-sentinel")
	require.NotContains(t, debugLogs.String(), "post-start-private-shard")
	require.NotContains(t, debugLogs.String(), "post-start-private-api-key")
}

func TestResponsePostStartLogsEachRetry(t *testing.T) {
	var count atomic.Int32
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	var logs bytes.Buffer
	client := startTraceClient(t, server.URL, &logs, true)
	client.responseRetryAttempts = 2
	client.retrySleep = func(context.Context, time.Duration) bool { return true }
	require.NoError(t, startTracePost(client))
	require.EqualValues(t, 2, count.Load())
	events := startTraceEvents(t, &logs)
	require.Len(t, events, 2)
	require.EqualValues(t, 1, events[0]["attempt"])
	require.EqualValues(t, 2, events[1]["attempt"])
}
