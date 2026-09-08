package main

import "os"

// Windows remains queue-only; this transport requires a private Unix runtime.
func crossSessionOwned(os.FileInfo) bool { return false }
