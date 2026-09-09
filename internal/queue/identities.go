package queue

import "crypto/rand"

// EnsureIdentities is an explicit mutation, called only under the broker's
// confirmed-holder gate. PeekTracked remains read-only for protocol-v1 peers.
func (s *Store) EnsureIdentities(rk RouteKey) error {
	_, err := s.rewritePendingMatches(rk, func(in storedInbound) bool { return in.RecordID == "" }, func(in *storedInbound) { in.RecordID = rand.Text() }, true)
	return err
}
