package ipc

// ReceiptHostVersion keeps host-supplied metadata bounded and safe in plain
// logs/status. An unavailable version is explicit, never guessed from fixtures.
func ReceiptHostVersion(version string) string {
	if version == "" || len(version) > 64 {
		return "unknown"
	}
	for _, c := range version {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '-' || c == '+' || c == '_') {
			return "unknown"
		}
	}
	return version
}

func ReceiptShapeHint(host string) string {
	if host == "" {
		return ""
	}
	return "receipt shapes may have changed (host " + ReceiptHostVersion(host) + "): run scripts/live-matrix/run.sh --collect-only --fixtures"
}
