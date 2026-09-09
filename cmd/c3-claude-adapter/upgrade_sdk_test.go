package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ServerSession.Close is a drain, not an interrupt. Own the raw transports and
// broker pipe so failed assertions and deadlines interrupt handlers BEFORE the
// SDK waits for them. Connect's context alone does not bound session.Close.
type upgradeSDKPair struct {
	ctx   context.Context
	conn  mcp.Connection
	abort func()
}

func connectUpgradeSDK(t *testing.T, a *adapter, srv *mcp.Server, opts *mcp.ServerSessionOptions) *upgradeSDKPair {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	a.runCtx = ctx
	ct, st := mcp.NewInMemoryTransports()
	client, err := ct.Connect(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	server, err := st.Connect(ctx)
	if err != nil {
		cancel()
		client.Close()
		t.Fatal(err)
	}
	var once sync.Once
	abort := func() {
		once.Do(func() {
			cancel()
			if conn := a.rawConn(); conn != nil {
				_ = conn.Close()
			}
			_ = client.Close()
			_ = server.Close()
		})
	}
	stop := context.AfterFunc(ctx, abort)
	var session *mcp.ServerSession
	t.Cleanup(func() {
		stop()
		abort()
		if session != nil {
			_ = session.Close()
		}
	})
	a.notifyTx = newNotifyTransport(&scriptedTransport{conn: server})
	session, err = srv.Connect(ctx, a.notifyTx, opts)
	if err != nil {
		t.Fatal(err)
	}
	return &upgradeSDKPair{ctx: ctx, conn: client, abort: abort}
}
