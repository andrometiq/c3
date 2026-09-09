# Debugging

## Attempt shadow (phase 1)

The broker keeps a memory-only shadow of tracked delivery attempts; nothing uses
it to decide delivery, retirement, status, or adapter behaviour. A disagreement
between an attempt's surviving member IDs and an authorised retirement logs
`attempt shadow DIVERGED token=… route=… expected=[…] removed=[…] reason=…` once
per retained token. The table keeps at most 128 entries per route and 4,096 globally,
evicting oldest terminal entries first and skipping new observations if only active
entries fill a cap. Channel observations expire lazily after 15 seconds with reason
`unobserved`; inbox fallback cannot be distinguished on this baseline. Legacy fetch
opens and immediately confirms a `fetch` attempt labelled `legacy_consume`; fetch
receipts join the table in phase 4. No new IPC operation, flag, or status field is added.
