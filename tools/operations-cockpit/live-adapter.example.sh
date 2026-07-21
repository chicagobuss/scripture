#!/bin/sh
# Copy to config/local/operations-cockpit/control.sh, make executable, then:
#   SCRIPTURE_OPS_ADAPTER=config/local/operations-cockpit/control.sh npm run start
#
# This is intentionally not a k8s implementation. Wire actions to a
# scenario-owned runner with an isolated namespace/prefix and an explicit
# execute/approval gate. Never accept an action string as shell code.
set -eu

case "${1:-}" in
  status)
    # Print exactly one bounded JSON snapshot matching fixture-state.json.
    # A live implementation may read kubectl/SSH/campaign artifacts here.
    cat "$(dirname "$0")/state.json"
    ;;
  action)
    case "${2:-}" in
      refresh|produce|pause-producer|resume-producer|kill-scribe-a|restart-scribe-a|promote-scribe-b|cut-store-b|restore-store-b|cleanup)
        echo "live adapter action not implemented; invoke a scenario-owned runner here" >&2
        exit 64
        ;;
      *) echo "unknown fixed cockpit action" >&2; exit 64 ;;
    esac
    ;;
  *) echo "usage: $0 status | action FIXED_ACTION" >&2; exit 64 ;;
esac
