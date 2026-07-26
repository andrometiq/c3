// Package version carries the C3 build version and the semver helpers the
// auto-updater uses to decide whether a GitHub release is newer than this
// binary.
//
// Version is injected at release-build time via
//
//	-ldflags "-X github.com/Andrometiq/c3/internal/version.Version=v1.0.0"
//
// (see scripts/package.sh, which the release workflow and `make dist` share). A
// plain `go build` / `go install` — including the /c3:build path — leaves it
// EMPTY; such a "dev" build reports "dev" and NEVER auto-updates (it has no
// release identity to compare against). Callers must go through Current()/IsDev()
// so the empty case is handled uniformly rather than reading Version directly.
package version

import (
	"strconv"
	"strings"
)

// Version is the release tag this binary was built from (e.g. "v1.0.0"), or ""
// for an uninjected local/dev build. Set only by the release pipeline's -ldflags.
var Version string

// devVersion is what Current() reports for an uninjected build.
const devVersion = "dev"

// Current returns the injected release version, or "dev" for a build with no
// version injected.
func Current() string {
	if Version == "" {
		return devVersion
	}
	return Version
}

// IsDev reports whether THIS binary is an uninjected dev build. A dev build's
// update checker is disabled: there is no release identity to compare against, so
// it would either nag forever or update to something it can't reason about.
func IsDev() bool {
	// Belt-and-braces via IsDevString: a build system that injects the literal
	// "dev" (instead of leaving Version empty) must still count as a dev build,
	// or the update checker would arm itself with a version it can't compare.
	return IsDevString(Version)
}

// IsDevString reports whether an arbitrary version string denotes a dev build
// ("" or "dev"). Used where the current version is passed around as a string
// (e.g. updater.Options.CurrentVersion) rather than read from Version.
func IsDevString(s string) bool {
	s = strings.TrimSpace(s)
	return s == "" || s == devVersion
}

// IsPrerelease reports whether a release tag denotes a prerelease. By the
// project's tagging convention (.github/workflows/release.yml treats any `v*-*`
// tag as a prerelease), a '-' after the version core marks a prerelease
// (e.g. "v1.0.0-rc1"). Prereleases are NEVER auto-installed.
func IsPrerelease(tag string) bool {
	core := strings.TrimPrefix(strings.TrimSpace(tag), "v")
	// Build metadata ('+') never marks a prerelease; strip it before the check.
	if i := strings.IndexByte(core, '+'); i >= 0 {
		core = core[:i]
	}
	return strings.Contains(core, "-")
}

// IsNewer reports whether candidate is a strictly newer version than current,
// by semver ordering (see Compare). A dev/"" current always sorts lowest, so any
// real release is "newer" — but the updater separately refuses to act on a dev
// build, so this is only reached for real current versions in practice.
func IsNewer(candidate, current string) bool {
	return Compare(candidate, current) > 0
}

// Compare returns -1 if a<b, 0 if a==b, +1 if a>b, using semver ordering on the
// leading vMAJOR.MINOR.PATCH core plus a simplified prerelease rule: a version
// WITH a prerelease suffix sorts BEFORE the same core without one
// (v1.0.0-rc1 < v1.0.0), and two prereleases on the same core compare
// lexically. Missing or non-numeric core fields are treated as 0, so a malformed
// or "dev" tag never panics — it just sorts low.
func Compare(a, b string) int {
	amaj, amin, apat, apre := parse(a)
	bmaj, bmin, bpat, bpre := parse(b)
	for _, d := range [][2]int{{amaj, bmaj}, {amin, bmin}, {apat, bpat}} {
		if d[0] != d[1] {
			if d[0] < d[1] {
				return -1
			}
			return 1
		}
	}
	// Cores are equal — apply the prerelease precedence rule.
	switch {
	case apre == "" && bpre == "":
		return 0
	case apre == "" && bpre != "":
		return 1 // a is the full release, b is a prerelease of it → a > b
	case apre != "" && bpre == "":
		return -1
	default:
		return strings.Compare(apre, bpre) // both prereleases: lexical is adequate for the guardrail
	}
}

// parse splits a tag into its numeric core (major, minor, patch) and prerelease
// suffix. It tolerates a leading "v", build metadata after '+', a missing patch
// or minor field, and non-numeric junk (all mapped to 0), so it can be called on
// any input including "dev" without panicking.
func parse(v string) (maj, min, pat int, pre string) {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if i := strings.IndexByte(v, '+'); i >= 0 { // drop build metadata
		v = v[:i]
	}
	if i := strings.IndexByte(v, '-'); i >= 0 { // split off prerelease
		pre = v[i+1:]
		v = v[:i]
	}
	parts := strings.Split(v, ".")
	get := func(idx int) int {
		if idx < len(parts) {
			if n, err := strconv.Atoi(strings.TrimSpace(parts[idx])); err == nil {
				return n
			}
		}
		return 0
	}
	return get(0), get(1), get(2), pre
}
