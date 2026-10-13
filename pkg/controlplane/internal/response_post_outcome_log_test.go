package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openai/tunnel-client/pkg/runtimeconfig"
	"github.com/openai/tunnel-client/pkg/tunnelctx"
	"github.com/openai/tunnel-client/pkg/types"
	"github.com/stretchr/testify/require"
)

func outcomeTraceClient(t *testing.T, url string, logs *bytes.Buffer, debug bool) *TunnelServiceClient {
	t.Helper()
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: level}))
	client, err := NewTunnelServiceClient(context.Background(),
		&runtimeconfig.ControlPlaneConfig{
			BaseURL:  mustParseURL(t, url),
			TunnelID: types.TunnelID("tunnel_post_outcome_test"),
			APIKey:   "post-outcome-private-api-key",
		}, nil, logger, &runtimeconfig.LoggingConfig{}, testMeterProvider)
	require.NoError(t, err)
	return client
}

func outcomeTracePost(client *TunnelServiceClient, ctx context.Context) error {
	ctx = tunnelctx.ContextWithShardToken(ctx, "post-outcome-private-shard")
	ctx = tunnelctx.ContextWithChannel(ctx, types.DefaultChannel)
	response := types.NewTunnelResponse(types.DefaultChannel,
		json.RawMessage("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"value\":\"private-outcome-body-sentinel\"}}"),
		http.StatusOK, nil)
	_, err := client.PostResponse(ctx, types.RequestID("post-outcome-test-request"), response)
	return err
}

func outcomeTraceEvents(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var events []map[string]any
	for _, line := range bytes.Split(logs.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var data map[string]any
		require.NoError(t, json.Unmarshal(line, &data))
		if data["msg"] == "control-plane response POST HTTP attempt returned" {
			events = append(events, data)
		}
	}
	return events
}

func TestResponsePostOutcomeDebugOnlyAndWireUnchanged(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		mu.Lock()
		bodies = append(bodies, append([]byte(nil), data...))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	var dbg, info bytes.Buffer
	require.NoError(t, outcomeTracePost(outcomeTraceClient(t, server.URL, &dbg, true), context.Background()))
	require.NoError(t, outcomeTracePost(outcomeTraceClient(t, server.URL, &info, false), context.Background()))
	mu.Lock()
	require.Len(t, bodies, 2)
	require.Equal(t, bodies[0], bodies[1])
	mu.Unlock()
	events := outcomeTraceEvents(t, &dbg)
	require.Len(t, events, 1)
	require.Equal(t, "DEBUG", events[0]["level"])
	require.EqualValues(t, 1, events[0]["attempt"])
	require.Equal(t, "http_response", events[0]["outcome"])
	require.EqualValues(t, 200, events[0]["status_code"])
	require.GreaterOrEqual(t, events[0]["elapsed_ms"], float64(0))
	require.Empty(t, outcomeTraceEvents(t, &info))
	for _, secret := range []string{"private-outcome-body-sentinel", "post-outcome-private-api-key", "post-outcome-private-shard"} {
		require.NotContains(t, dbg.String(), secret)
	}
}

func TestResponsePostOutcomeRetryStatus(t *testing.T) {
	var requests atomic.Int32
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	var logs bytes.Buffer
	client := outcomeTraceClient(t, server.URL, &logs, true)
	client.responseRetryAttempts = 2
	client.retrySleep = func(context.Context, time.Duration) bool { return true }
	require.NoError(t, outcomeTracePost(client, context.Background()))
	require.EqualValues(t, 2, requests.Load())
	events := outcomeTraceEvents(t, &logs)
	require.Len(t, events, 2)
	require.EqualValues(t, 1, events[0]["attempt"])
	require.EqualValues(t, 503, events[0]["status_code"])
	require.EqualValues(t, 2, events[1]["attempt"])
	require.EqualValues(t, 200, events[1]["status_code"])
}

func TestResponsePostOutcomeContextCancellation(t *testing.T) {
	seen := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case seen <- struct{}{}:
		default:
		}
		<-release
	}))
	var logs bytes.Buffer
	client := outcomeTraceClient(t, server.URL, &logs, true)
	client.responseRetryAttempts = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- outcomeTracePost(client, ctx) }()
	select {
	case <-seen:
		cancel()
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("server did not receive request")
	}
	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "canceled"))
	case <-time.After(3 * time.Second):
		t.Fatal("request not canceled")
	}
	events := outcomeTraceEvents(t, &logs)
	require.Len(t, events, 1)
	require.Equal(t, "transport_error", events[0]["outcome"])
	require.Equal(t, "canceled", events[0]["failure_category"])
	require.NotContains(t, logs.String(), "private-outcome-body-sentinel")
	require.NotContains(t, logs.String(), "post-outcome-private-shard")
}

func TestResponsePostOutcomeDeadlineCategory(t *testing.T) {
	requestArrived := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case requestArrived <- struct{}{}:
		default:
		}
		<-release
	}))
	var logs bytes.Buffer
	client := outcomeTraceClient(t, server.URL, &logs, true)
	client.responseRetryAttempts = 1
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- outcomeTracePost(client, ctx) }()
	select {
	case <-requestArrived:
	case <-time.After(2 * time.Second):
		t.Fatal("request not received")
	}
	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "deadline"))
	case <-time.After(3 * time.Second):
		t.Fatal("context deadline was not honored")
	}
	events := outcomeTraceEvents(t, &logs)
	require.Len(t, events, 1)
	require.Equal(t, "transport_error", events[0]["outcome"])
	require.Equal(t, "timeout", events[0]["failure_category"])
	require.GreaterOrEqual(t, events[0]["elapsed_ms"], float64(0))
}

func TestResponsePostOutcomeNotAcceptedOnHTTP404(t *testing.T) {
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNotFound)
	}))
	var logs bytes.Buffer
	client := outcomeTraceClient(t, server.URL, &logs, true)
	require.NoError(t, outcomeTracePost(client, context.Background()))
	events := outcomeTraceEvents(t, &logs)
	require.Len(t, events, 1)
	require.Equal(t, "http_response", events[0]["outcome"])
	require.EqualValues(t, http.StatusNotFound, events[0]["status_code"])
	require.NotContains(t, logs.String(), "private-outcome-body-sentinel")
}
