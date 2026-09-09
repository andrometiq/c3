package broker

// A self-exec reconnect may start claim transfer immediately after the durable
// removal. Never re-enter Routes.RLock from inside withConfirmedHolder: a
// pending transfer writer makes that nested read lock deadlock. Reconcile on
// the same route worker AFTER the ownership-protected mutation returns.
func (w *RouteWorker) reconcileUpgradeAck(before *shadowRows) {
	if before.ok {
		w.shadowRemoval(*before, w.shadowConsumeToken, "live_ack")
	}
}
func (w *RouteWorker) beforeUpgradeAckRemoval() {
	if hook := w.broker.upgrades.beforeAckRemoval; hook != nil {
		hook()
	}
}
