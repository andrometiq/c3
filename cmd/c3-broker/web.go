package main

import (
	"encoding/json"
	"fmt"

	"github.com/Andrometiq/c3/internal/ipc"
)

func runWeb(args []string) error {
	if len(args) != 1 || args[0] != "link" && args[0] != "ca" {
		return fmt.Errorf("usage: c3-broker web link|ca")
	}
	conn, err := dialBroker()
	if err != nil {
		return err
	}
	defer conn.Close()
	if args[0] == "ca" {
		if err := conn.WriteJSON(ipc.WebCAReq{Op: ipc.OpWebCA}); err != nil {
			return fmt.Errorf("write web_ca: %w", err)
		}
		raw, err := conn.ReadFrame()
		if err != nil {
			return fmt.Errorf("read web_ca_reply: %w", err)
		}
		var reply ipc.WebCAReply
		if err := json.Unmarshal(raw, &reply); err != nil {
			return fmt.Errorf("parse web_ca_reply: %w", err)
		}
		if !reply.OK {
			return fmt.Errorf("%s", reply.Err)
		}
		fmt.Printf("web CA certificate sent to the configured Telegram operator DM (SHA-256 %s)\n", reply.Fingerprint)
		return nil
	}
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
