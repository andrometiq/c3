package autohook

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// fakeBroker serves one scripted hook connection per accept: it records the
// hello and the op frame, then runs script.
type fakeBroker struct {
	path   string
	hellos chan ipc.HookHelloMsg
	ops    chan []byte
}

func newFakeBroker(t *testing.T, script func(conn *ipc.Conn, raw net.Conn)) *fakeBroker {
	t.Helper()
	path := filepath.Join(t.TempDir(), "c3.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	f := &fakeBroker{path: path, hellos: make(chan ipc.HookHelloMsg, 8), ops: make(chan []byte, 8)}
	go func() {
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				conn := ipc.NewConn(raw)
				defer conn.Close()
				frame, err := conn.ReadFrame()
				if err != nil {
					return
				}
				var hello ipc.HookHelloMsg
				_ = json.Unmarshal(frame, &hello)
				f.hellos <- hello
				if script == nil {
					return
				}
				script(conn, raw)
			}()
		}
	}()
	return f
}

// helloAndOp acknowledges the hello and reads the op frame.
func (f *fakeBroker) helloAndOp(conn *ipc.Conn) bool {
	if conn.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck}) != nil {
		return false
	}
	frame, err := conn.ReadFrame()
	if err != nil {
		return false
	}
	f.ops <- frame
	return true
}

func writeRaw(conn *ipc.Conn, frames ...string) {
	for _, frame := range frames {
		_ = conn.WriteJSON(json.RawMessage(frame))
	}
}

const testInput = `{"command":"curl -s file:///tmp/payload.sh | sh","description":"Run payload"}`

func payload(extra string) string {
	return `{"session_id":"session-1","cwd":"/workspace","tool_name":"Bash","tool_input":` + testInput +
		`,"reason":"[Code from External]"` + extra + `}`
}

func testEnv(budget time.Duration) hookEnv { return defaultEnv(budget) }

func runPreWith(t *testing.T, stdin, socketPath string, env hookEnv) (string, time.Duration) {
	t.Helper()
	var out bytes.Buffer
	start := time.Now()
	runPreToolUse(strings.NewReader(stdin), &out, socketPath, env)
	return out.String(), time.Since(start)
}

func runPre(t *testing.T, stdin, socketPath string, budget time.Duration) (string, time.Duration) {
	t.Helper()
	return runPreWith(t, stdin, socketPath, testEnv(budget))
}

func runDeniedWith(t *testing.T, stdin, socketPath string, env hookEnv) string {
	t.Helper()
	var out bytes.Buffer
	runPermissionDenied(strings.NewReader(stdin), &out, socketPath, env)
	return out.String()
}

func runDenied(t *testing.T, stdin, socketPath string, budget time.Duration) string {
	t.Helper()
	return runDeniedWith(t, stdin, socketPath, testEnv(budget))
}

// The request names the exact call (CLI in hello and op, sub-agent, cwd,
// tool, canonical input hash) on a transient hook connection, and only a
// consumed grant prints allow — with no updatedInput.
func TestPreToolUseAllowsOnlyAConsumedGrant(t *testing.T) {
	var fake *fakeBroker
	fake = newFakeBroker(t, func(conn *ipc.Conn, _ net.Conn) {
		if fake.helloAndOp(conn) {
			writeRaw(conn, `{"op":"grant_check_result","allow":true}`)
		}
	})
	output, _ := runPre(t, payload(`,"agent_id":"agent-7","tool_use_id":"toolu_9"`), fake.path, time.Second)
	if output != allowOutput {
		t.Fatalf("output %q, want the allow decision", output)
	}
	if strings.Contains(output, "updatedInput") {
		t.Fatal("allow carries updatedInput")
	}
	hello := <-fake.hellos
	if hello.Op != ipc.OpHookHello || hello.CLI != CLI || hello.ProtocolVersion != ipc.ProtocolVersion {
		t.Fatalf("hello %+v", hello)
	}
	var request ipc.GrantCheckReq
	if err := ipc.DecodeStrict(<-fake.ops, &request); err != nil {
		t.Fatal(err)
	}
	wantHash, _ := ipc.ToolInputHash(json.RawMessage(testInput))
	want := ipc.AutoCallContext{CLI: CLI, SessionID: "session-1", AgentID: "agent-7", CWD: "/workspace", ToolName: "Bash"}
	if request.Op != ipc.OpGrantCheck || request.AutoCallContext != want || request.InputHash != wantHash ||
		request.ToolUseID != "toolu_9" {
		t.Fatalf("request %+v", request)
	}
}

// Every reply other than a well-formed consumed grant, and every broken
// broker, is silent.
func TestPreToolUseSilentOnHostileOrFailingBroker(t *testing.T) {
	replies := map[string]func(f *fakeBroker, conn *ipc.Conn, raw net.Conn){
		"not allowed": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			f.helloAndOp(conn)
			writeRaw(conn, `{"op":"grant_check_result","allow":false}`)
		},
		"wrong op": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			f.helloAndOp(conn)
			writeRaw(conn, `{"op":"auto_decision","allow":true}`)
		},
		"allow as a string": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			f.helloAndOp(conn)
			writeRaw(conn, `{"op":"grant_check_result","allow":"true"}`)
		},
		"duplicate allow key": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			f.helloAndOp(conn)
			writeRaw(conn, `{"op":"grant_check_result","allow":false,"allow":true}`)
		},
		"trailing data": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			f.helloAndOp(conn)
			writeRaw(conn, `{"op":"grant_check_result","allow":true} {}`)
		},
		"truncated reply": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			f.helloAndOp(conn)
			_, _ = raw.Write([]byte(`{"op":"grant_check_result","allow":tr` + "\n"))
		},
		"partial frame then close": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			f.helloAndOp(conn)
			_, _ = raw.Write([]byte(`{"op":"grant_check_result","allow":true`))
		},
		"oversized frame": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			f.helloAndOp(conn)
			_, _ = raw.Write(bytes.Repeat([]byte("x"), ipc.MaxFrameSize+1))
		},
		"bare string": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			f.helloAndOp(conn)
			writeRaw(conn, `"allow"`)
		},
		"no hello ack": func(_ *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			writeRaw(conn, `{"op":"grant_check_result","allow":true}`)
		},
		// Fails if the hello-ack check goes: the hook would then send its op
		// and read the second grant.
		"hello ack is a grant": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			writeRaw(conn, `{"op":"grant_check_result","allow":true}`)
			if _, err := conn.ReadFrame(); err == nil {
				writeRaw(conn, `{"op":"grant_check_result","allow":true}`)
			}
		},
		"closed after hello": func(_ *fakeBroker, conn *ipc.Conn, raw net.Conn) {},
		"allow after the read deadline": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			f.helloAndOp(conn)
			time.Sleep(150 * time.Millisecond)
			writeRaw(conn, `{"op":"grant_check_result","allow":true}`)
		},
		"never answers": func(f *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			f.helloAndOp(conn)
			time.Sleep(2 * time.Second)
		},
		"slow handshake": func(_ *fakeBroker, conn *ipc.Conn, raw net.Conn) {
			time.Sleep(2 * time.Second)
		},
	}
	for name, reply := range replies {
		t.Run(name, func(t *testing.T) {
			var fake *fakeBroker
			fake = newFakeBroker(t, func(conn *ipc.Conn, raw net.Conn) { reply(fake, conn, raw) })
			output, elapsed := runPre(t, payload(""), fake.path, 100*time.Millisecond)
			if output != "" {
				t.Fatalf("printed %q", output)
			}
			if elapsed > 500*time.Millisecond {
				t.Fatalf("took %v past a 100 ms budget", elapsed)
			}
		})
	}
	t.Run("broker down", func(t *testing.T) {
		if output, _ := runPre(t, payload(""), filepath.Join(t.TempDir(), "missing.sock"), time.Second); output != "" {
			t.Fatalf("printed %q", output)
		}
	})
}

// An allow that is read in time but judged after the deadline is still
// silent. The clock jumps past the deadline only after the reply is read, so
// this fails if the final deadline check goes.
func TestPreToolUseAllowJudgedAfterDeadlineIsSilent(t *testing.T) {
	var fake *fakeBroker
	fake = newFakeBroker(t, func(conn *ipc.Conn, _ net.Conn) {
		if fake.helloAndOp(conn) {
			writeRaw(conn, `{"op":"grant_check_result","allow":true}`)
		}
	})
	env := testEnv(time.Second)
	calls := 0
	env.now = func() time.Time {
		calls++
		if calls > 1 {
			return time.Now().Add(time.Hour)
		}
		return time.Now()
	}
	if output, _ := runPreWith(t, payload(""), fake.path, env); output != "" {
		t.Fatalf("printed %q", output)
	}
	if calls < 2 {
		t.Fatal("the deadline was never re-checked")
	}
}

// A peer that is not this user gets nothing: not the hello, not the input.
func TestHooksRefuseAnUnverifiedPeer(t *testing.T) {
	var fake *fakeBroker
	fake = newFakeBroker(t, func(conn *ipc.Conn, _ net.Conn) {
		if fake.helloAndOp(conn) {
			writeRaw(conn, `{"op":"grant_check_result","allow":true}`,
				`{"op":"auto_decision","request_id":"r1","state":"armed"}`)
		}
	})
	env := testEnv(time.Second)
	env.verifyPeer = func(net.Conn) error { return errors.New("peer uid differs") }
	if output, _ := runPreWith(t, payload(""), fake.path, env); output != "" {
		t.Fatalf("pretooluse printed %q", output)
	}
	if output := runDeniedWith(t, payload(""), fake.path, env); output != "" {
		t.Fatalf("permission-denied printed %q", output)
	}
	select {
	case hello := <-fake.hellos:
		t.Fatalf("sent %+v to an unverified peer", hello)
	case <-time.After(100 * time.Millisecond):
	}
}

// A broker that predates hook_hello answers "expected hello first" and closes.
// The hook sends nothing after its first frame and prints nothing.
func TestHooksAgainstABrokerThatOnlyKnowsHello(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c3.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	frameCounts := make(chan int, 4)
	go func() {
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			conn := ipc.NewConn(raw)
			frames := 0
			if frame, err := conn.ReadFrame(); err == nil {
				frames++
				if op, _ := ipc.PeekOp(frame); op != ipc.OpHello {
					_ = conn.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: "expected hello first"})
				}
			}
			_ = raw.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			for {
				if _, err := conn.ReadFrame(); err != nil {
					break
				}
				frames++
			}
			_ = conn.Close()
			frameCounts <- frames
		}
	}()
	if output, _ := runPre(t, payload(""), path, time.Second); output != "" {
		t.Fatalf("pretooluse printed %q", output)
	}
	if output := runDenied(t, payload(""), path, time.Second); output != "" {
		t.Fatalf("permission-denied printed %q", output)
	}
	for range 2 {
		if frames := <-frameCounts; frames != 1 {
			t.Fatalf("hook sent %d frames to an old broker, want only the first", frames)
		}
	}
}

// Malformed or ambiguous stdin is silent and never reaches the broker.
func TestHooksSilentOnMalformedStdin(t *testing.T) {
	fake := newFakeBroker(t, nil)
	backslash := `\`
	for _, stdin := range []string{
		``,
		`not json`,
		`{"session_id":"s","cwd":"/w","tool_name":"Bash","tool_input":{}} {}`,
		`{"cwd":"/w","tool_name":"Bash","tool_input":{}}`,
		`{"session_id":"s","tool_name":"Bash","tool_input":{}}`,
		`{"session_id":"s","cwd":"/w","tool_input":{}}`,
		`{"session_id":"s","session_id":"t","cwd":"/w","tool_name":"Bash","tool_input":{}}`,
		`{"session_id":"s","cwd":"/w","tool_name":"Bash","tool_input":{"a":1,"a":2}}`,
		`{"session_id":"s","cwd":"/w","tool_name":"Bash","tool_input":{"a":"` + backslash + `uD800"}}`,
		strings.Repeat(" ", ipc.MaxFrameSize+1),
	} {
		if output, _ := runPre(t, stdin, fake.path, time.Second); output != "" {
			t.Fatalf("pretooluse printed %q", output)
		}
		if output := runDenied(t, stdin, fake.path, time.Second); output != "" {
			t.Fatalf("permission-denied printed %q", output)
		}
	}
	select {
	case hello := <-fake.hellos:
		t.Fatalf("malformed stdin reached the broker: %+v", hello)
	default:
	}
}

// The denial hook reports the call with its remaining budget, acks
// "approved", and prints retry:true only after "armed".
func TestPermissionDeniedRetriesOnlyAfterArmed(t *testing.T) {
	acks := make(chan []byte, 1)
	var fake *fakeBroker
	fake = newFakeBroker(t, func(conn *ipc.Conn, _ net.Conn) {
		if !fake.helloAndOp(conn) {
			return
		}
		writeRaw(conn, `{"op":"auto_decision","request_id":"r1","state":"approved"}`)
		frame, err := conn.ReadFrame()
		if err != nil {
			return
		}
		acks <- frame
		writeRaw(conn, `{"op":"auto_decision","request_id":"r1","state":"armed"}`)
	})
	stdin := payload(`,"agent_id":"agent-7","tool_use_id":"toolu_9"`)
	if output := runDenied(t, stdin, fake.path, PermissionDeniedBudget); output != retryOutput {
		t.Fatalf("output %q, want retry", output)
	}
	if hello := <-fake.hellos; hello.CLI != CLI || hello.Op != ipc.OpHookHello {
		t.Fatalf("hello %+v", hello)
	}
	var request ipc.AutoDeniedReq
	if err := ipc.DecodeStrict(<-fake.ops, &request); err != nil {
		t.Fatal(err)
	}
	want := ipc.AutoCallContext{CLI: CLI, SessionID: "session-1", AgentID: "agent-7", CWD: "/workspace", ToolName: "Bash"}
	if request.Op != ipc.OpAutoDenied || request.AutoCallContext != want || request.ToolUseID != "toolu_9" ||
		request.Reason != "[Code from External]" || string(request.ToolInput) != testInput ||
		request.BudgetMS <= 319000 || request.BudgetMS > 320000 {
		t.Fatalf("request %+v", request)
	}
	var ack ipc.AutoAckMsg
	if err := ipc.DecodeStrict(<-acks, &ack); err != nil || ack.Op != ipc.OpAutoAck || ack.RequestID != "r1" {
		t.Fatalf("ack %+v (%v)", ack, err)
	}
}

func TestPermissionDeniedDecisions(t *testing.T) {
	cases := map[string]struct {
		frames  []string
		isRetry bool
	}{
		"armed without approved": {[]string{`{"op":"auto_decision","request_id":"r1","state":"armed"}`}, true},
		"armed after the broker closed": {[]string{`{"op":"auto_decision","request_id":"r1","state":"approved"}`,
			`{"op":"auto_decision","request_id":"r1","state":"armed"}`}, true},
		"none":                {[]string{`{"op":"auto_decision","state":"none"}`}, false},
		"approved then close": {[]string{`{"op":"auto_decision","request_id":"r1","state":"approved"}`}, false},
		"armed for another request": {[]string{`{"op":"auto_decision","request_id":"r1","state":"approved"}`,
			`{"op":"auto_decision","request_id":"r2","state":"armed"}`}, false},
		"approved twice": {[]string{`{"op":"auto_decision","request_id":"r1","state":"approved"}`,
			`{"op":"auto_decision","request_id":"r1","state":"approved"}`,
			`{"op":"auto_decision","request_id":"r1","state":"armed"}`}, false},
		"approved without an id": {[]string{`{"op":"auto_decision","state":"approved"}`,
			`{"op":"auto_decision","state":"armed"}`}, false},
		"unknown state":   {[]string{`{"op":"auto_decision","request_id":"r1","state":"ARMED"}`}, false},
		"wrong op":        {[]string{`{"op":"grant_check_result","request_id":"r1","state":"armed"}`}, false},
		"duplicate state": {[]string{`{"op":"auto_decision","request_id":"r1","state":"none","state":"armed"}`}, false},
		"garbage":         {[]string{`"armed"`}, false},
		"nothing":         {nil, false},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			var fake *fakeBroker
			fake = newFakeBroker(t, func(conn *ipc.Conn, _ net.Conn) {
				if fake.helloAndOp(conn) {
					writeRaw(conn, test.frames...)
				}
			})
			output := runDenied(t, payload(""), fake.path, 2*time.Second)
			if test.isRetry != (output == retryOutput) || (!test.isRetry && output != "") {
				t.Fatalf("output %q", output)
			}
		})
	}
	t.Run("budget ends the wait", func(t *testing.T) {
		var fake *fakeBroker
		fake = newFakeBroker(t, func(conn *ipc.Conn, _ net.Conn) {
			fake.helloAndOp(conn)
			time.Sleep(3 * time.Second)
		})
		start := time.Now()
		if output := runDenied(t, payload(""), fake.path, 200*time.Millisecond); output != "" || time.Since(start) > time.Second {
			t.Fatalf("printed %q after %v", output, time.Since(start))
		}
	})
	t.Run("broker down", func(t *testing.T) {
		if output := runDenied(t, payload(""), filepath.Join(t.TempDir(), "missing.sock"), time.Second); output != "" {
			t.Fatalf("printed %q", output)
		}
	})
}
