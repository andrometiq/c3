#!/bin/sh

# Print the GitHub release notes for an annotated v* tag.
#
# The tag message is the release note. Its first line names the tag
# ("vX.Y.Z — summary"); everything after the first blank line is the note users
# read on the release page, printed exactly as written. Refused, so no release
# goes out without notes or with notes silently cut: a lightweight tag, a first
# line that does not name the tag, a line 2 that is not empty, an empty note,
# and an unclosed code fence.
#
# Usage: scripts/release-notes.sh <tag>
# Preview locally after `git tag -a` and before pushing the tag.
set -eu

if [ "$#" -ne 1 ]; then
	echo "usage: $0 <tag>" >&2
	exit 2
fi
tag=$1
ref="refs/tags/$tag"

if ! kind=$(git cat-file -t "$ref" 2>/dev/null); then
	echo "release-notes: no tag $tag" >&2
	exit 1
fi
if [ "$kind" != tag ]; then
	echo "release-notes: $tag is a lightweight tag; re-create it with git tag -a and write the release notes in its message" >&2
	exit 1
fi

# git's subject is the whole first paragraph joined into one line, so it must
# equal the first line: otherwise a missing blank line silently drops notes.
subject=$(git for-each-ref "$ref" --format='%(contents:subject)')
first=$(git for-each-ref "$ref" --format='%(contents)' | awk 'NF { sub(/\r$/, ""); print; exit }')
case " $first " in
*" $tag "*) ;;
*)
	printf "release-notes: the first line of %s's message is \"%s\"; it must be '%s — <summary>', then a blank line, then the notes\n" "$tag" "$first" "$tag" >&2
	exit 1
	;;
esac
if [ "$subject" != "$first" ]; then
	printf "release-notes: line 2 of %s's message must be empty (no spaces or tabs); git drops everything before the first empty line from the notes\n" "$tag" >&2
	exit 1
fi

body=$(git for-each-ref "$ref" --format='%(contents:body)')
if [ -z "$(printf '%s' "$body" | tr -d '[:space:]')" ]; then
	echo "release-notes: tag $tag has no notes; they go after the first blank line of the tag message" >&2
	exit 1
fi
if [ $(($(printf '%s\n' "$body" | grep -c -E '^[[:space:]]*(```|~~~)') % 2)) -ne 0 ]; then
	echo "release-notes: tag $tag has an unclosed code fence; it would swallow the rest of the release page" >&2
	exit 1
fi
printf '%s\n' "$body"
