package dbos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type writeCommand struct {
	messageType int
	data        []byte
	response    chan error
}

type mockWebSocketServer struct {
	server      *httptest.Server
	upgrader    websocket.Upgrader
	connMu      sync.Mutex
	conn        *websocket.Conn
	closed      atomic.Bool
	messages    chan []byte
	pings       chan struct{}
	writeCmds   chan writeCommand
	stopHandler chan struct{}
	ignorePings atomic.Bool
}

func newMockWebSocketServer() *mockWebSocketServer {
	m := &mockWebSocketServer{
		upgrader:    websocket.Upgrader{},
		messages:    make(chan []byte, 100),
		pings:       make(chan struct{}, 100),
		writeCmds:   make(chan writeCommand, 10),
		stopHandler: make(chan struct{}),
	}

	m.server = httptest.NewServer(http.HandlerFunc(m.handleWebSocket))
	return m
}

func (m *mockWebSocketServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {

	if m.closed.Load() {
		http.Error(w, "Server closed", http.StatusServiceUnavailable)
		return
	}

	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	m.connMu.Lock()

	if m.conn != nil {
		m.conn.Close()
	}
	m.conn = conn
	m.connMu.Unlock()

	defer func() {
		m.connMu.Lock()
		if m.conn == conn {
			m.conn = nil
		}
		m.connMu.Unlock()
		conn.Close()
	}()

	// We need to handle pings manually since we can't use the ping handler

	pingReceived := make(chan struct{}, 10)

	conn.SetPingHandler(func(string) error {
		select {
		case m.pings <- struct{}{}:
		default:
		}
		select {
		case pingReceived <- struct{}{}:
		default:
		}
		return nil
	})

	readDone := make(chan error, 1)
	go func() {
		defer close(readDone)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				fmt.Printf("WebSocket read error: %v\n", err)
				readDone <- err
				return
			}
			select {
			case m.messages <- data:
			default:
			}
		}
	}()

	for {
		select {
		case <-m.stopHandler:
			fmt.Println("WebSocket connection closed by stop signal")
			return

		case err := <-readDone:
			fmt.Printf("WebSocket connection closed by read error: %v\n", err)
			return

		case writeCmd := <-m.writeCmds:

			err := conn.WriteMessage(writeCmd.messageType, writeCmd.data)
			if writeCmd.response != nil {
				select {
				case writeCmd.response <- err:
				default:
				}
			}
			if err != nil {
				fmt.Printf("WebSocket write error: %v\n", err)
				return
			}

		case <-pingReceived:

			if !m.ignorePings.Load() {
				err := conn.WriteMessage(websocket.PongMessage, nil)
				if err != nil {
					fmt.Printf("WebSocket pong write error: %v\n", err)
					return
				}
			}
		}
	}
}

func (m *mockWebSocketServer) getUrl() string {
	return "ws" + strings.TrimPrefix(m.server.URL, "http")
}

func (m *mockWebSocketServer) close() {
	m.closed.Store(true)

	select {
	case m.stopHandler <- struct{}{}:
	default:
	}
}

func (m *mockWebSocketServer) shutdown() {
	m.close()
	m.server.Close()
}

func (m *mockWebSocketServer) restart() {

	m.closed.Store(false)

	select {
	case <-m.stopHandler:
	default:
	}

drainLoop:
	for {
		select {
		case cmd := <-m.writeCmds:
			if cmd.response != nil {
				select {
				case cmd.response <- fmt.Errorf("server restarting"):
				default:
				}
			}
		default:
			break drainLoop
		}
	}
}

func (m *mockWebSocketServer) waitForConnection(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m.connMu.Lock()
		hasConn := m.conn != nil
		m.connMu.Unlock()
		if hasConn {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (m *mockWebSocketServer) sendTextMessage(data []byte) error {
	m.connMu.Lock()
	hasConn := m.conn != nil
	m.connMu.Unlock()

	if !hasConn {
		return fmt.Errorf("no connection")
	}

	response := make(chan error, 1)
	cmd := writeCommand{
		messageType: websocket.TextMessage,
		data:        data,
		response:    response,
	}

	select {
	case m.writeCmds <- cmd:
		select {
		case err := <-response:
			return err
		case <-time.After(1 * time.Second):
			return fmt.Errorf("write timeout")
		}
	case <-time.After(1 * time.Second):
		return fmt.Errorf("write command queue full")
	}
}

func (m *mockWebSocketServer) sendBinaryMessage(data []byte) error {
	// Check if we have a connection without blocking
	m.connMu.Lock()
	hasConn := m.conn != nil
	m.connMu.Unlock()

	if !hasConn {
		return fmt.Errorf("no connection")
	}

	response := make(chan error, 1)
	cmd := writeCommand{
		messageType: websocket.BinaryMessage,
		data:        data,
		response:    response,
	}

	select {
	case m.writeCmds <- cmd:

		select {
		case err := <-response:
			return err
		case <-time.After(1 * time.Second):
			return fmt.Errorf("write timeout")
		}
	case <-time.After(1 * time.Second):
		return fmt.Errorf("write command queue full")
	}
}

func (m *mockWebSocketServer) sendCloseMessage(code int, text string) error {
	// Check if we have a connection without blocking
	m.connMu.Lock()
	hasConn := m.conn != nil
	m.connMu.Unlock()

	if !hasConn {
		return fmt.Errorf("no connection")
	}

	message := websocket.FormatCloseMessage(code, text)

	response := make(chan error, 1)
	cmd := writeCommand{
		messageType: websocket.CloseMessage,
		data:        message,
		response:    response,
	}

	select {
	case m.writeCmds <- cmd:

		select {
		case err := <-response:

			m.connMu.Lock()
			if m.conn != nil {
				m.conn.Close()
				m.conn = nil
			}
			m.connMu.Unlock()
			return err
		case <-time.After(1 * time.Second):
			return fmt.Errorf("write timeout")
		}
	case <-time.After(1 * time.Second):
		return fmt.Errorf("write command queue full")
	}
}

func TestConductorReconnection(t *testing.T) {
	t.Run("ServerRestart", func(t *testing.T) {
		defer verifyNoLeaks(t)

		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		config := conductorConfig{
			url:     mockServer.getUrl(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}

		conductor, err := newConductor(dbosCtx, config)
		require.NoError(t, err)

		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		conductor.launch()

		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish initial connection")

		initialPings := 0
		timeout := time.After(1 * time.Second)
	collectInitialPings:
		for {
			select {
			case <-mockServer.pings:
				initialPings++
			case <-timeout:
				break collectInitialPings
			}
		}
		assert.Greater(t, initialPings, 0, "Should receive initial pings")
		fmt.Printf("Received %d initial pings\n", initialPings)

		fmt.Println("Closing server connection")
		mockServer.close()

		time.Sleep(500 * time.Millisecond)

		fmt.Println("Restarting server")
		mockServer.restart()

		assert.True(t, mockServer.waitForConnection(10*time.Second), "Should reconnect after server restart")

		reconnectPings := 0
		timeout2 := time.After(1 * time.Second)
	collectReconnectPings:
		for {
			select {
			case <-mockServer.pings:
				reconnectPings++
			case <-timeout2:
				break collectReconnectPings
			}
		}
		assert.Greater(t, reconnectPings, 0, "Should receive pings after reconnection")
		t.Logf("Received %d pings after reconnection", reconnectPings)

		cancel()

		time.Sleep(500 * time.Millisecond)
	})

	t.Run("TestBinaryMessage", func(t *testing.T) {
		defer verifyNoLeaks(t)

		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		config := conductorConfig{
			url:     mockServer.getUrl(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}

		conductor, err := newConductor(dbosCtx, config)
		require.NoError(t, err)

		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		conductor.launch()

		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish initial connection")

		initialPings := 0
		timeout := time.After(1 * time.Second)
	collectInitialPings:
		for {
			select {
			case <-mockServer.pings:
				initialPings++
			case <-timeout:
				break collectInitialPings
			}
		}
		assert.Greater(t, initialPings, 0, "Should receive initial pings")
		fmt.Printf("Received %d initial pings\n", initialPings)

		fmt.Println("Sending binary message to trigger disconnect")
		err = mockServer.sendBinaryMessage([]byte{0xDE, 0xAD, 0xBE, 0xEF})
		assert.NoError(t, err, "Should send binary message successfully")

		time.Sleep(200 * time.Millisecond)

		assert.True(t, mockServer.waitForConnection(10*time.Second), "Should reconnect after receiving binary message")

		reconnectPings := 0
		timeout2 := time.After(1 * time.Second)
	collectReconnectPings:
		for {
			select {
			case <-mockServer.pings:
				reconnectPings++
			case <-timeout2:
				break collectReconnectPings
			}
		}
		assert.Greater(t, reconnectPings, 0, "Should receive pings after reconnection from binary message")
		t.Logf("Received %d pings after reconnection from binary message", reconnectPings)

		cancel()

		time.Sleep(500 * time.Millisecond)
	})

	t.Run("TestConductorPingTimeout", func(t *testing.T) {
		defer verifyNoLeaks(t)

		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		config := conductorConfig{
			url:     mockServer.getUrl(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}

		conductor, err := newConductor(dbosCtx, config)
		require.NoError(t, err)

		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		conductor.launch()

		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish initial connection")

		initialPings := 0
		timeout := time.After(1 * time.Second)
	collectInitialPings:
		for {
			select {
			case <-mockServer.pings:
				initialPings++
			case <-timeout:
				break collectInitialPings
			}
		}
		assert.Greater(t, initialPings, 0, "Should receive initial pings")
		fmt.Printf("Received %d initial pings\n", initialPings)

		fmt.Println("Server stopping pong responses")
		mockServer.ignorePings.Store(true)

		time.Sleep(conductor.pingTimeout + 100*time.Millisecond)

		fmt.Println("Server resuming pong responses")
		mockServer.ignorePings.Store(false)

		assert.True(t, mockServer.waitForConnection(10*time.Second), "Should reconnect after ping timeout")

		reconnectPings := 0
		timeout2 := time.After(1 * time.Second)
	collectReconnectPings:
		for {
			select {
			case <-mockServer.pings:
				reconnectPings++
			case <-timeout2:
				break collectReconnectPings
			}
		}
		assert.Greater(t, reconnectPings, 0, "Should receive pings after reconnection")
		t.Logf("Received %d pings after reconnection", reconnectPings)

		cancel()

		time.Sleep(500 * time.Millisecond)
	})

	t.Run("CloseMessages", func(t *testing.T) {
		defer verifyNoLeaks(t)

		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		config := conductorConfig{
			url:     mockServer.getUrl(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}

		conductor, err := newConductor(dbosCtx, config)
		require.NoError(t, err)

		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		conductor.launch()

		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish initial connection")

		testCases := []struct {
			code   int
			reason string
			name   string
		}{
			{websocket.CloseGoingAway, "server going away", "CloseGoingAway"},
			{websocket.CloseAbnormalClosure, "abnormal closure", "CloseAbnormalClosure"},
		}

		for _, tc := range testCases {
			t.Logf("Testing %s (code %d)", tc.name, tc.code)

			assert.True(t, mockServer.waitForConnection(5*time.Second), "Should have stable connection before %s", tc.name)
			time.Sleep(300 * time.Millisecond)

			beforePings := 0
			timeout := time.After(200 * time.Millisecond)
		collectBeforePings:
			for {
				select {
				case <-mockServer.pings:
					beforePings++
				case <-timeout:
					break collectBeforePings
				}
			}
			assert.Greater(t, beforePings, 0, "Should receive pings before %s", tc.name)

			err = mockServer.sendCloseMessage(tc.code, tc.reason)
			assert.NoError(t, err, "Should send %s close message successfully", tc.name)

			time.Sleep(300 * time.Millisecond)

			assert.True(t, mockServer.waitForConnection(10*time.Second), "Should reconnect after %s", tc.name)

			afterPings := 0
			timeout2 := time.After(200 * time.Millisecond)
		collectAfterPings:
			for {
				select {
				case <-mockServer.pings:
					afterPings++
				case <-timeout2:
					break collectAfterPings
				}
			}
			assert.Greater(t, afterPings, 0, "Should receive pings after reconnection from %s", tc.name)
		}

		cancel()

		time.Sleep(500 * time.Millisecond)
	})
}

func TestConductorExecutorInfo(t *testing.T) {
	runExecutorInfo := func(t *testing.T, metadata map[string]any) executorInfoResponse {
		t.Helper()

		mockServer := newMockWebSocketServer()
		t.Cleanup(mockServer.shutdown)

		config := conductorConfig{
			url:              mockServer.getUrl(),
			apiKey:           "test-key",
			appName:          "test-app",
			executorMetadata: metadata,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)

		dbosCtx := &dbosContext{
			ctx:                ctx,
			logger:             slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
			applicationVersion: "v-test",
			executorId:         "executor-test",
		}

		cond, err := newConductor(dbosCtx, config)
		require.NoError(t, err)
		cond.pingInterval = 100 * time.Millisecond
		cond.pingTimeout = 200 * time.Millisecond
		cond.reconnectWait = 100 * time.Millisecond

		cond.launch()
		t.Cleanup(func() { cond.shutdown(2 * time.Second) })
		require.True(t, mockServer.waitForConnection(5*time.Second), "Should establish connection")

		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"executor_info","request_id":"req-info-1"}`)))

		deadline := time.After(5 * time.Second)
		for {
			select {
			case raw := <-mockServer.messages:
				var base baseMessage
				if err := json.Unmarshal(raw, &base); err == nil && base.Type == executorInfo {
					var resp executorInfoResponse
					require.NoError(t, json.Unmarshal(raw, &resp))
					return resp
				}
			case <-deadline:
				t.Fatal("timed out waiting for executor_info response")
			}
		}
	}

	t.Run("WithMetadata", func(t *testing.T) {
		resp := runExecutorInfo(t, map[string]any{
			"region":   "us-east-1",
			"instance": float64(42),
		})
		assert.Equal(t, "req-info-1", resp.RequestId)
		assert.Equal(t, executorInfo, resp.Type)
		assert.Equal(t, "executor-test", resp.ExecutorId)
		assert.Equal(t, "v-test", resp.ApplicationVersion)
		assert.Equal(t, "go", resp.Language)
		assert.Equal(t, map[string]any{
			"region":   "us-east-1",
			"instance": float64(42),
		}, resp.ExecutorMetadata)
	})

	t.Run("WithoutMetadata", func(t *testing.T) {
		resp := runExecutorInfo(t, nil)
		assert.Equal(t, "req-info-1", resp.RequestId)
		assert.Equal(t, "executor-test", resp.ExecutorId)
		assert.Nil(t, resp.ExecutorMetadata)
	})
}

func TestConductorAlertHandler(t *testing.T) {
	t.Run("WithHandler", func(t *testing.T) {
		defer verifyNoLeaks(t)

		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		config := conductorConfig{
			url:     mockServer.getUrl(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var handlerName, handlerMessage string
		var handlerMetadata map[string]string
		handlerCalled := make(chan struct{}, 1)

		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
			alertHandler: func(name string, message string, metadata map[string]string) {
				handlerName = name
				handlerMessage = message
				handlerMetadata = metadata
				handlerCalled <- struct{}{}
			},
		}

		cond, err := newConductor(dbosCtx, config)
		require.NoError(t, err)
		cond.pingInterval = 100 * time.Millisecond
		cond.pingTimeout = 200 * time.Millisecond
		cond.reconnectWait = 100 * time.Millisecond

		cond.launch()
		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish connection")

		alertMsg := `{"type":"alert","request_id":"req-123","name":"test-alert","message":"something happened","metadata":{"key1":"val1","key2":"val2"}}`
		err = mockServer.sendTextMessage([]byte(alertMsg))
		require.NoError(t, err)

		select {
		case <-handlerCalled:
		case <-time.After(5 * time.Second):
			t.Fatal("alert handler was not called")
		}

		assert.Equal(t, "test-alert", handlerName)
		assert.Equal(t, "something happened", handlerMessage)
		assert.Equal(t, map[string]string{"key1": "val1", "key2": "val2"}, handlerMetadata)

		select {
		case respData := <-mockServer.messages:
			var resp alertConductorResponse
			err = json.Unmarshal(respData, &resp)
			require.NoError(t, err)
			assert.True(t, resp.Success)
			assert.Equal(t, "req-123", resp.RequestId)
			assert.Equal(t, alertMessage, resp.Type)
			assert.Nil(t, resp.ErrorMessage)
		case <-time.After(5 * time.Second):
			t.Fatal("did not receive alert response")
		}

		cancel()
		time.Sleep(500 * time.Millisecond)
	})

	t.Run("WithoutHandler", func(t *testing.T) {
		defer verifyNoLeaks(t)

		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		config := conductorConfig{
			url:     mockServer.getUrl(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}

		cond, err := newConductor(dbosCtx, config)
		require.NoError(t, err)
		cond.pingInterval = 100 * time.Millisecond
		cond.pingTimeout = 200 * time.Millisecond
		cond.reconnectWait = 100 * time.Millisecond

		cond.launch()
		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish connection")

		alertMsg := `{"type":"alert","request_id":"req-456","name":"unhandled","message":"no handler","metadata":{}}`
		err = mockServer.sendTextMessage([]byte(alertMsg))
		require.NoError(t, err)

		select {
		case respData := <-mockServer.messages:
			var resp alertConductorResponse
			err = json.Unmarshal(respData, &resp)
			require.NoError(t, err)
			assert.True(t, resp.Success)
			assert.Equal(t, "req-456", resp.RequestId)
		case <-time.After(5 * time.Second):
			t.Fatal("did not receive alert response")
		}

		cancel()
		time.Sleep(500 * time.Millisecond)
	})

	t.Run("HandlerPanic", func(t *testing.T) {
		defer verifyNoLeaks(t)

		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		config := conductorConfig{
			url:     mockServer.getUrl(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
			alertHandler: func(name string, message string, metadata map[string]string) {
				panic("handler exploded")
			},
		}

		cond, err := newConductor(dbosCtx, config)
		require.NoError(t, err)
		cond.pingInterval = 100 * time.Millisecond
		cond.pingTimeout = 200 * time.Millisecond
		cond.reconnectWait = 100 * time.Millisecond

		cond.launch()
		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish connection")

		alertMsg := `{"type":"alert","request_id":"req-789","name":"panic-alert","message":"trigger panic","metadata":{}}`
		err = mockServer.sendTextMessage([]byte(alertMsg))
		require.NoError(t, err)

		select {
		case respData := <-mockServer.messages:
			var resp alertConductorResponse
			err = json.Unmarshal(respData, &resp)
			require.NoError(t, err)
			assert.False(t, resp.Success)
			assert.Equal(t, "req-789", resp.RequestId)
			assert.NotNil(t, resp.ErrorMessage)
			assert.Contains(t, *resp.ErrorMessage, "panic in alert handler")
		case <-time.After(5 * time.Second):
			t.Fatal("did not receive alert response")
		}

		cancel()
		time.Sleep(500 * time.Millisecond)
	})
}

func TestConductorScheduleHandlers(t *testing.T) {
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, schedulerPollingInterval: 100 * time.Millisecond})
	NewWorkflow(dbosCtx, testWorkflowForSchedule)
	require.NoError(t, dbosCtx.Launch())

	const baseSchedule = "cond-base-schedule"
	require.NoError(t, CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: baseSchedule,
		Schedule:     "0 0 0 1 1 *",
	}, WithScheduleContext("hello")))

	mockServer := newMockWebSocketServer()
	t.Cleanup(mockServer.shutdown)

	cond, err := newConductor(dbosCtx.(*dbosContext), conductorConfig{
		url:     mockServer.getUrl(),
		apiKey:  "test-key",
		appName: "test-app",
	})
	require.NoError(t, err)
	cond.pingInterval = 100 * time.Millisecond
	cond.pingTimeout = 200 * time.Millisecond
	cond.reconnectWait = 100 * time.Millisecond
	cond.launch()
	t.Cleanup(func() { cond.shutdown(2 * time.Second) })
	require.True(t, mockServer.waitForConnection(5*time.Second))

	expect := func(t *testing.T, wantType messageType) []byte {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case raw := <-mockServer.messages:
				var base baseMessage
				if err := json.Unmarshal(raw, &base); err == nil && base.Type == wantType {
					return raw
				}

			case <-deadline:
				t.Fatalf("timed out waiting for response of type %s", wantType)
			}
		}
	}

	t.Run("list_schedules", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"list_schedules","request_id":"r1","body":{}}`)))
		var resp listSchedulesConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, listSchedulesMessage), &resp))
		require.Equal(t, "r1", resp.RequestId)
		require.Nil(t, resp.ErrorMessage)
		require.Equal(t, 1, len(resp.Output))
		require.Equal(t, baseSchedule, resp.Output[0].ScheduleName)
		require.NotNil(t, resp.Output[0].Context, "load_context defaults to true")
	})

	t.Run("list_schedules_no_context", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"list_schedules","request_id":"r2","body":{"load_context":false}}`)))
		var resp listSchedulesConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, listSchedulesMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.Equal(t, 1, len(resp.Output))
		require.Nil(t, resp.Output[0].Context, "load_context=false should omit context")
	})

	t.Run("get_schedule", func(t *testing.T) {
		req := fmt.Sprintf(`{"type":"get_schedule","request_id":"r3","schedule_name":%q}`, baseSchedule)
		require.NoError(t, mockServer.sendTextMessage([]byte(req)))
		var resp getScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, getScheduleMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.NotNil(t, resp.Output)
		require.Equal(t, baseSchedule, resp.Output.ScheduleName)
	})

	t.Run("get_schedule_missing", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"get_schedule","request_id":"r4","schedule_name":"does-not-exist"}`)))
		var resp getScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, getScheduleMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.Nil(t, resp.Output)
	})

	t.Run("pause_resume_schedule", func(t *testing.T) {
		req := fmt.Sprintf(`{"type":"pause_schedule","request_id":"r5","schedule_name":%q}`, baseSchedule)
		require.NoError(t, mockServer.sendTextMessage([]byte(req)))
		var pauseResp pauseScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, pauseScheduleMessage), &pauseResp))
		require.True(t, pauseResp.Success)
		got, err := GetSchedule(dbosCtx, baseSchedule)
		require.NoError(t, err)
		require.Equal(t, ScheduleStatusPaused, got.Status)

		req = fmt.Sprintf(`{"type":"resume_schedule","request_id":"r6","schedule_name":%q}`, baseSchedule)
		require.NoError(t, mockServer.sendTextMessage([]byte(req)))
		var resumeResp resumeScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, resumeScheduleMessage), &resumeResp))
		require.True(t, resumeResp.Success)
		got, err = GetSchedule(dbosCtx, baseSchedule)
		require.NoError(t, err)
		require.Equal(t, ScheduleStatusActive, got.Status)
	})

	t.Run("backfill_schedule", func(t *testing.T) {

		const fastSchedule = "cond-backfill-schedule"
		require.NoError(t, CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
			ScheduleName: fastSchedule,
			Schedule:     "*/1 * * * * *",
		}))
		t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, fastSchedule) })

		startISO := time.Now().Add(-3 * time.Second).Format(time.RFC3339Nano)
		endISO := time.Now().Format(time.RFC3339Nano)
		req := fmt.Sprintf(`{"type":"backfill_schedule","request_id":"r7","schedule_name":%q,"start":%q,"end":%q}`,
			fastSchedule, startISO, endISO)
		require.NoError(t, mockServer.sendTextMessage([]byte(req)))
		var resp backfillScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, backfillScheduleMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.NotEmpty(t, resp.WorkflowIds)
	})

	t.Run("trigger_schedule", func(t *testing.T) {
		req := fmt.Sprintf(`{"type":"trigger_schedule","request_id":"r8","schedule_name":%q}`, baseSchedule)
		require.NoError(t, mockServer.sendTextMessage([]byte(req)))
		var resp triggerScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, triggerScheduleMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.NotNil(t, resp.WorkflowId)
		require.Contains(t, *resp.WorkflowId, baseSchedule)
	})

	t.Run("trigger_schedule_missing", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"trigger_schedule","request_id":"r9","schedule_name":"missing"}`)))
		var resp triggerScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, triggerScheduleMessage), &resp))
		require.Nil(t, resp.WorkflowId)
		require.NotNil(t, resp.ErrorMessage)
	})
}

func conductorAggregatesWorkflow(_ DbosContext, in string) (string, error) {
	return in, nil
}

func TestConductorWorkflowAggregatesHandler(t *testing.T) {
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true})
	conductorAggregatesWF := NewWorkflow(dbosCtx, conductorAggregatesWorkflow)
	require.NoError(t, dbosCtx.Launch())

	for i := 0; i < 3; i++ {
		h, err := conductorAggregatesWF(dbosCtx, fmt.Sprintf("ok-%d", i))
		require.NoError(t, err)
		_, err = h.GetResult()
		require.NoError(t, err)
	}

	mockServer := newMockWebSocketServer()
	t.Cleanup(mockServer.shutdown)

	cond, err := newConductor(dbosCtx.(*dbosContext), conductorConfig{
		url:     mockServer.getUrl(),
		apiKey:  "test-key",
		appName: "test-app",
	})
	require.NoError(t, err)
	cond.pingInterval = 100 * time.Millisecond
	cond.pingTimeout = 200 * time.Millisecond
	cond.reconnectWait = 100 * time.Millisecond
	cond.launch()
	t.Cleanup(func() { cond.shutdown(2 * time.Second) })
	require.True(t, mockServer.waitForConnection(5*time.Second))

	expect := func(t *testing.T, wantType messageType) []byte {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case raw := <-mockServer.messages:
				var base baseMessage
				if err := json.Unmarshal(raw, &base); err == nil && base.Type == wantType {
					return raw
				}

			case <-deadline:
				t.Fatalf("timed out waiting for response of type %s", wantType)
			}
		}
	}

	t.Run("group_by_status", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"get_workflow_aggregates","request_id":"agg1","body":{"group_by_status":true}}`)))
		var resp getWorkflowAggregatesConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, getWorkflowAggregatesMessage), &resp))
		require.Equal(t, "agg1", resp.RequestId)
		require.Nil(t, resp.ErrorMessage)
		require.NotEmpty(t, resp.Output)
		var successCount int64
		for _, row := range resp.Output {
			require.NotNil(t, row.Group["status"])
			if *row.Group["status"] == string(WorkflowStatusSuccess) {
				successCount = row.Count
			}
		}
		require.Equal(t, int64(3), successCount)
	})

	t.Run("no_group_by_returns_error", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"get_workflow_aggregates","request_id":"agg2","body":{}}`)))
		var resp getWorkflowAggregatesConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, getWorkflowAggregatesMessage), &resp))
		require.NotNil(t, resp.ErrorMessage)
	})
}

func conductorStepAggWorkflow(ctx DbosContext, _ string) (string, error) {
	return Run(ctx, stepAggOK, WithStepName("condAggStep"))
}

func TestConductorStepAggregatesHandler(t *testing.T) {
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true})
	conductorStepAggWF := NewWorkflow(dbosCtx, conductorStepAggWorkflow)
	require.NoError(t, dbosCtx.Launch())

	for i := 0; i < 3; i++ {
		h, err := conductorStepAggWF(dbosCtx, fmt.Sprintf("ok-%d", i))
		require.NoError(t, err)
		_, err = h.GetResult()
		require.NoError(t, err)
	}

	mockServer := newMockWebSocketServer()
	t.Cleanup(mockServer.shutdown)

	cond, err := newConductor(dbosCtx.(*dbosContext), conductorConfig{
		url:     mockServer.getUrl(),
		apiKey:  "test-key",
		appName: "test-app",
	})
	require.NoError(t, err)
	cond.pingInterval = 100 * time.Millisecond
	cond.pingTimeout = 200 * time.Millisecond
	cond.reconnectWait = 100 * time.Millisecond
	cond.launch()
	t.Cleanup(func() { cond.shutdown(2 * time.Second) })
	require.True(t, mockServer.waitForConnection(5*time.Second))

	expect := func(t *testing.T, wantType messageType) []byte {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case raw := <-mockServer.messages:
				var base baseMessage
				if err := json.Unmarshal(raw, &base); err == nil && base.Type == wantType {
					return raw
				}
			case <-deadline:
				t.Fatalf("timed out waiting for response of type %s", wantType)
			}
		}
	}

	t.Run("get_step_aggregates", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"get_step_aggregates","request_id":"sa1","body":{"group_by_function_name":true,"select_count":true}}`)))
		var resp getStepAggregatesConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, getStepAggregatesMessage), &resp))
		require.Equal(t, "sa1", resp.RequestId)
		require.Nil(t, resp.ErrorMessage)
		var stepCount int64
		for _, r := range resp.Output {
			require.NotNil(t, r.Group["function_name"])
			if *r.Group["function_name"] == "condAggStep" {
				require.NotNil(t, r.Count)
				stepCount = *r.Count
			}
		}
		require.Equal(t, int64(3), stepCount)
	})

	t.Run("get_step_aggregates_no_group_errors", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"get_step_aggregates","request_id":"sa2","body":{"select_count":true}}`)))
		var resp getStepAggregatesConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, getStepAggregatesMessage), &resp))
		require.NotNil(t, resp.ErrorMessage)
	})
}

func conductorPrivateModeStep(_ context.Context, in string) (string, error) {
	return "step-" + in, nil
}

func conductorPrivateModeWorkflow(ctx DbosContext, in string) (string, error) {
	return Run(ctx, func(c context.Context) (string, error) {
		return conductorPrivateModeStep(c, in)
	})
}

func TestConductorPrivateMode(t *testing.T) {
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true})
	conductorPrivateModeWF := NewWorkflow(dbosCtx, conductorPrivateModeWorkflow)
	require.NoError(t, dbosCtx.Launch())

	h, err := conductorPrivateModeWF(dbosCtx, "secret")
	require.NoError(t, err)
	_, err = h.GetResult()
	require.NoError(t, err)
	wfId := h.GetWorkflowId()

	mockServer := newMockWebSocketServer()
	t.Cleanup(mockServer.shutdown)

	cond, err := newConductor(dbosCtx.(*dbosContext), conductorConfig{
		url:     mockServer.getUrl(),
		apiKey:  "test-key",
		appName: "test-app",
	})
	require.NoError(t, err)
	cond.pingInterval = 100 * time.Millisecond
	cond.pingTimeout = 200 * time.Millisecond
	cond.reconnectWait = 100 * time.Millisecond
	cond.launch()
	t.Cleanup(func() { cond.shutdown(2 * time.Second) })
	require.True(t, mockServer.waitForConnection(5*time.Second))

	expect := func(t *testing.T, wantType messageType) []byte {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case raw := <-mockServer.messages:
				var base baseMessage
				if err := json.Unmarshal(raw, &base); err == nil && base.Type == wantType {
					return raw
				}
			case <-deadline:
				t.Fatalf("timed out waiting for response of type %s", wantType)
			}
		}
	}

	t.Run("get_workflow_loads_io", func(t *testing.T) {
		msg := fmt.Sprintf(`{"type":"get_workflow","request_id":"g1","workflow_id":%q,"load_input":true,"load_output":true}`, wfId)
		require.NoError(t, mockServer.sendTextMessage([]byte(msg)))
		var resp getWorkflowConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, getWorkflowMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.NotNil(t, resp.Output)
		require.NotNil(t, resp.Output.Input)
		require.NotNil(t, resp.Output.Output)
	})

	t.Run("get_workflow_private", func(t *testing.T) {
		msg := fmt.Sprintf(`{"type":"get_workflow","request_id":"g2","workflow_id":%q,"load_input":false,"load_output":false}`, wfId)
		require.NoError(t, mockServer.sendTextMessage([]byte(msg)))
		var resp getWorkflowConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, getWorkflowMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.NotNil(t, resp.Output)
		require.Nil(t, resp.Output.Input)
		require.Nil(t, resp.Output.Output)
	})

	t.Run("list_steps_loads_output", func(t *testing.T) {
		msg := fmt.Sprintf(`{"type":"list_steps","request_id":"s1","workflow_id":%q,"load_output":true}`, wfId)
		require.NoError(t, mockServer.sendTextMessage([]byte(msg)))
		var resp listStepsConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, listStepsMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.NotNil(t, resp.Output)
		require.NotEmpty(t, *resp.Output)
		require.NotNil(t, (*resp.Output)[0].Output)
	})

	t.Run("list_steps_private", func(t *testing.T) {
		msg := fmt.Sprintf(`{"type":"list_steps","request_id":"s2","workflow_id":%q,"load_output":false}`, wfId)
		require.NoError(t, mockServer.sendTextMessage([]byte(msg)))
		var resp listStepsConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, listStepsMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.NotNil(t, resp.Output)
		require.NotEmpty(t, *resp.Output)
		require.Nil(t, (*resp.Output)[0].Output)
	})
}
