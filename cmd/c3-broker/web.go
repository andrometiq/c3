package main

import (
	"encoding/json"
	"fmt"

	"github.com/Andrometiq/c3/internal/ipc"
)

func runWeb(args []string) error {
	if len(args) != 1 || args[0] != "link" {
		return fmt.Errorf("usage: c3-broker web link")
	}
	conn, err := dialBroker()
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.WriteJSON(ipc.WebLoginLinkReq{Op: ipc.OpWebLoginLink}); err != nil {
		return fmt.Errorf("write web_login_link: %w", err)
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		return fmt.Errorf("read web_login_link_reply: %w", err)
	}
	var reply ipc.WebLoginLinkReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		return fmt.Errorf("parse web_login_link_reply: %w", err)
	}
	if !reply.OK {
		return fmt.Errorf("%s", reply.Err)
	}
	fmt.Println("web login link sent to the configured Telegram operator DM")
	return nil
}
