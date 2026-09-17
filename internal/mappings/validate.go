package mappings

import (
	"fmt"
	"strings"
)

// Validate returns nil if the MappingsFile is internally consistent, or a
// concrete error describing the first inconsistency found.
//
// Checks:
//   - schema_version is recognized.
//   - For each channel: default_group, if set, exists in groups.
//   - For each topic: its group, if set, exists in groups.
//   - For each mapping: its channel exists.
//
// This does NOT validate against Telegram (e.g. that chat_ids are real groups
// the bot has access to). Network validation lives in the channel module.
func (mf *MappingsFile) Validate() error {
	if mf == nil {
		return fmt.Errorf("mappings: nil file")
	}
	if mf.SchemaVersion != 1 {
		return fmt.Errorf("mappings: unsupported schema_version %d (want 1)", mf.SchemaVersion)
	}
	for chanName, cc := range mf.Channels {
		if cc.DefaultGroup != "" {
			if _, ok := cc.Groups[cc.DefaultGroup]; !ok {
				return fmt.Errorf("mappings: channel %q default_group %q not in groups", chanName, cc.DefaultGroup)
			}
		}
		for _, tp := range cc.Topics {
			if tp.Group == "" {
				continue
			}
			if _, ok := cc.Groups[tp.Group]; !ok {
				return fmt.Errorf("mappings: channel %q topic %q references unknown group %q", chanName, tp.Name, tp.Group)
			}
		}
		for botName, bot := range cc.Bots {
			if strings.TrimSpace(botName) == "" {
				return fmt.Errorf("mappings: channel %q has an empty bots key", chanName)
			}
			if strings.TrimSpace(bot.BotToken) == "" {
				return fmt.Errorf("mappings: channel %q bots.%s missing bot_token", chanName, botName)
			}
		}
	}
	for cwd, m := range mf.Mappings {
		if _, ok := mf.Channel(m.Channel); !ok {
			return fmt.Errorf("mappings: cwd %q maps to unknown channel %q", cwd, m.Channel)
		}
	}
	return nil
}
