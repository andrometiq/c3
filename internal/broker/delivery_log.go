package broker

import "fmt"

func deliveredLog(key RouteKey, messageID int64, s *Stub, transport, token string) string {
	return fmt.Sprintf("delivered chan=%s topic=%s msg=%d to cli=%s conn=%d transport=%s token=%s", key.Channel, TopicKeyStr(key), messageID, s.CLI, s.ConnID, transport, token)
}
