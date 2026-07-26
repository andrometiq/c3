package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/Andrometiq/c3/internal/ipc"
)

// runPair is the `c3-broker pair` subcommand. Sends an OpPairModeStart to
// the running broker and prints the generated code so the operator can
// type it into the bot from their Telegram client.
//
// Usage:
//
//	c3-broker pair                  # arm DM pairing
//	c3-broker pair dm               # arm DM pairing (explicit)
//	c3-broker pair group <chat_id>  # arm group pairing for chat_id
//
// The matching slash command is /c3:pair (plugins/c3/commands/c3-pair.md).
func runPair(args []string) error {
	target := "dm"
	var chatID int64
	if len(args) >= 1 {
		switch args[0] {
		case "dm":
			target = "dm"
		case "group":
			target = "group"
			if len(args) < 2 {
				return fmt.Errorf("`pair group` requires a chat_id argument")
			}
			n, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("parse chat_id %q: %w", args[1], err)
			}
			chatID = n
		default:
			return fmt.Errorf("unknown pair target %q (want \"dm\" or \"group <chat_id>\")", args[0])
		}
	}

	conn, err := dialBroker()
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.WriteJSON(ipc.PairModeStartReq{
		Op:     ipc.OpPairModeStart,
		Target: target,
		ChatID: chatID,
	}); err != nil {
		return fmt.Errorf("write pair_mode_start: %w", err)
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		return fmt.Errorf("read pair_mode_reply: %w", err)
	}
	var resp ipc.PairModeReplyMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("parse pair_mode_reply: %w", err)
	}
	if !resp.OK {
		return fmt.Errorf("broker refused: %s", resp.Err)
	}
	switch resp.Target {
	case "dm":
		fmt.Printf("Send `%s` to your bot now to pair (DM, %ds window).\n", resp.Code, resp.TTLSec)
	case "group":
		fmt.Printf("In group chat_id=%d, send `%s` from any account to pair the group (%ds window).\n",
			resp.ChatID, resp.Code, resp.TTLSec)
	default:
		fmt.Printf("Pairing code: %s (ttl %ds)\n", resp.Code, resp.TTLSec)
	}
	return nil
}
