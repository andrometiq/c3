package main

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func multiRouteToolRequest(t *testing.T, name string, args map[string]any) *mcp.CallToolRequest {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	return &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: name, Arguments: raw}}
}

func multiRouteResultText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	if result == nil || len(result.Content) == 0 {
		t.Fatalf("empty tool result: %+v", result)
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("tool content type=%T, want *mcp.TextContent", result.Content[0])
	}
	return text.Text
}

func TestMultiRouteToolSchemas(t *testing.T) {
	a := newAdapter()
	srv := a.buildMCPServer()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = srv.Run(ctx, serverTransport) }()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	list, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	schemas := map[string]string{}
	descriptions := map[string]string{}
	for _, tool := range list.Tools {
		raw, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		schemas[tool.Name] = string(raw)
		descriptions[tool.Name] = tool.Description
	}
	if _, ok := schemas["output"]; !ok {
		t.Fatal("tools/list missing output")
	}
	for tool, property := range map[string]string{
		"attach": "add", "detach": "target", "output": "target",
		"reply": "channel", "react": "channel", "edit_message": "channel",
		"fetch_queue": "channel",
	} {
		schema, ok := schemas[tool]
		if !ok {
			t.Errorf("tools/list missing %s", tool)
			continue
		}
		if !strings.Contains(schema, "\""+property+"\"") {
			t.Errorf("%s schema missing %q: %s", tool, property, schema)
		}
	}
	if !strings.Contains(descriptions["reply"], "whenever it makes the reply easier to read") {
		t.Errorf("reply description lost formatting nudge: %q", descriptions["reply"])
	}
	if _, ok := schemas["send_typing"]; ok {
		t.Fatal("send_typing must remain absent")
	}
}

func TestOriginTagAbsentSingleRoute(t *testing.T) {
	a := newAdapter()
	topicID := int64(281)
	telegram := ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &topicID, Name: "c3"}
	a.setRouteState([]ipc.RouteRef{telegram}, &telegram)
	in := c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &topicID}
	if got := a.originTag(&in); got != "" {
		t.Fatalf("single-route origin tag=%q, want empty", got)
	}
}

func TestOriginTagPresentMultiRoute(t *testing.T) {
	a := newAdapter()
	topicID := int64(281)
	telegram := ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &topicID, Name: "c3"}
	web := ipc.RouteRef{Channel: "web", ChatID: 42, Name: "web"}
	a.setRouteState([]ipc.RouteRef{telegram, web}, &web)
	if got := a.originTag(&c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &topicID}); got != "[telegram · c3] " {
		t.Fatalf("telegram origin tag=%q", got)
	}
	if got := a.originTag(&c3types.Inbound{Channel: "web", ChatID: 42}); got != "[web] " {
		t.Fatalf("web origin tag=%q", got)
	}
	rendered := a.renderFetchedMessages([]c3types.Inbound{{Channel: "web", ChatID: 42, Text: "hello"}}, 0, "web")
	if !strings.HasPrefix(rendered, "[web] hello") {
		t.Fatalf("fetched origin tag missing: %q", rendered)
	}
}

func TestOutputToolSetsRoute(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	a := newAdapter()
	a.conn = ipc.NewConn(left)
	a.brokerVersion.Store(int64(ipc.ProtocolVersion))
	peer := ipc.NewConn(right)
	topicID := int64(281)
	telegram := ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &topicID, Name: "c3"}
	web := ipc.RouteRef{Channel: "web", ChatID: 42, Name: "web"}
	a.setRouteState([]ipc.RouteRef{telegram, web}, &web)

	type outcome struct {
		result *mcp.CallToolResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := a.toolSetOutput(context.Background(), multiRouteToolRequest(t, "output", map[string]any{"target": "telegram"}))
		done <- outcome{result: result, err: err}
	}()
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var req ipc.SetOutputRouteReq
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatal(err)
	}
	if req.Op != ipc.OpSetOutputRoute || req.Target != "telegram" {
		t.Fatalf("set-output request=%+v", req)
	}
	respRaw, _ := json.Marshal(ipc.SetOutputRouteResp{Op: ipc.OpSetOutputRouteResult, OK: true, Output: &telegram})
	a.dispatchSetOutputRouteResult(respRaw)
	got := <-done
	if got.err != nil || got.result.IsError || multiRouteResultText(t, got.result) != "output route → c3" {
		t.Fatalf("output result=%+v err=%v", got.result, got.err)
	}
	if a.outputRoute == nil || a.outputRoute.Channel != "telegram" {
		t.Fatalf("output state=%+v", a.outputRoute)
	}

	go func() {
		result, err := a.toolSetOutput(context.Background(), multiRouteToolRequest(t, "output", map[string]any{"target": "web"}))
		done <- outcome{result: result, err: err}
	}()
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	errRaw, _ := json.Marshal(ipc.SetOutputRouteResp{Op: ipc.OpSetOutputRouteResult, Err: "selected route is not held"})
	a.dispatchSetOutputRouteResult(errRaw)
	got = <-done
	if got.err != nil || !got.result.IsError || !strings.Contains(multiRouteResultText(t, got.result), "selected route is not held") {
		t.Fatalf("output error result=%+v err=%v", got.result, got.err)
	}
	if a.outputRoute == nil || a.outputRoute.Channel != "telegram" {
		t.Fatalf("error changed output state=%+v", a.outputRoute)
	}
}

func TestDetachTargetReleasesOne(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	a := newAdapter()
	a.conn = ipc.NewConn(left)
	a.brokerVersion.Store(int64(ipc.ProtocolVersion))
	a.lastAttach = &ipc.AttachReq{Op: ipc.OpAttach, Expr: "+web"}
	peer := ipc.NewConn(right)
	topicID := int64(281)
	telegram := ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &topicID, Name: "c3"}
	web := ipc.RouteRef{Channel: "web", ChatID: 42, Name: "web"}
	a.setRouteState([]ipc.RouteRef{telegram, web}, &web)

	type outcome struct {
		result *mcp.CallToolResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := a.toolDetach(context.Background(), multiRouteToolRequest(t, "detach", map[string]any{"target": "web"}))
		done <- outcome{result: result, err: err}
	}()
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var req ipc.ReleaseReq
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatal(err)
	}
	if req.Op != ipc.OpRelease || req.Target != "web" {
		t.Fatalf("release request=%+v", req)
	}
	respRaw, _ := json.Marshal(ipc.ReleaseResp{Op: ipc.OpReleaseResult, OK: true, Routes: []ipc.RouteRef{telegram}, Output: &telegram})
	a.dispatchReleaseResult(respRaw)
	got := <-done
	if got.err != nil || got.result.IsError {
		t.Fatalf("target detach result=%+v err=%v", got.result, got.err)
	}
	if len(a.routes) != 1 || a.routes[0].Channel != "telegram" || a.outputRoute == nil || a.outputRoute.Channel != "telegram" {
		t.Fatalf("target detach state routes=%+v output=%+v", a.routes, a.outputRoute)
	}

	go func() {
		result, err := a.toolDetach(context.Background(), multiRouteToolRequest(t, "detach", nil))
		done <- outcome{result: result, err: err}
	}()
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	got = <-done
	if got.err != nil || got.result.IsError {
		t.Fatalf("bare detach result=%+v err=%v", got.result, got.err)
	}
	if len(a.routes) != 0 || a.outputRoute != nil || a.lastAttach != nil {
		t.Fatalf("bare detach did not clear state: routes=%+v output=%+v attach=%+v", a.routes, a.outputRoute, a.lastAttach)
	}
}
