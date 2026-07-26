package updater

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// LatestReleaseURL is the GitHub REST endpoint for the newest PUBLISHED,
// non-draft, non-prerelease release of the C3 repo. Unauthenticated; GitHub's
// anonymous rate limit (60 req/hr/IP) is far above the ~6h check cadence.
//
// It is a var (not a const) solely so tests can point FetchLatest at a loopback
// httptest server; production never reassigns it.
var LatestReleaseURL = "https://api.github.com/repos/Andrometiq/c3/releases/latest"

// maxAPIBody caps the release-API JSON we read, so a hostile/huge response can't
// exhaust memory. A release payload is a few KB; 1 MiB is generous headroom.
const maxAPIBody = 1 << 20

// FetchLatest queries the GitHub releases API for the latest release and decodes
// it. Network failures, non-200 status, and decode errors are returned as errors
// for the caller to treat as "check failed" (the broker logs and moves on — a
// failed check is never surfaced to the user). client bounds the request; pass
// DefaultClient() for the standard conservative timeout.
func FetchLatest(ctx context.Context, client *http.Client) (*Release, error) {
	if client == nil {
		client = DefaultClient()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, LatestReleaseURL, nil)
	if err != nil {
		return nil, err
	}
	// GitHub asks for an explicit API version + a User-Agent; without a UA it 403s.
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "c3-updater")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Read a little of the body for the error message but don't fail on it.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("github releases API: status %d: %s", resp.StatusCode, string(snippet))
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxAPIBody))
	if err != nil {
		return nil, err
	}
	return ParseRelease(data)
}

// ParseRelease decodes a GitHub release JSON payload. Network-free so the parse
// contract is unit-testable against a fixture.
func ParseRelease(data []byte) (*Release, error) {
	var rel Release
	if err := json.Unmarshal(data, &rel); err != nil {
		return nil, fmt.Errorf("decode release JSON: %w", err)
	}
	if rel.TagName == "" {
		return nil, fmt.Errorf("release JSON has no tag_name")
	}
	return &rel, nil
}
