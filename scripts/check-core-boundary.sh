#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

if ! grep -Eq '^#!\[no_std\]$' crates/herdr-flow-core/src/lib.rs; then
  echo "herdr-flow-core must remain no_std" >&2
  exit 1
fi

# This bare-metal target has no standard library. Compiling the complete core
# dependency graph for it rejects `extern crate std` as well as implicit std use.
cargo check --locked -p herdr-flow-core --target thumbv7em-none-eabihf

actual_dependencies=$(
  cargo tree --locked -p herdr-flow-core --depth 1 --prefix none \
    | tail -n +2 \
    | sed -E 's/ v[0-9].*$//' \
    | LC_ALL=C sort -u
)
expected_dependencies='serde'

if [[ "$actual_dependencies" != "$expected_dependencies" ]]; then
  echo "herdr-flow-core direct dependencies changed." >&2
  echo "Expected allowlist:" >&2
  echo "$expected_dependencies" >&2
  echo "Actual:" >&2
  echo "$actual_dependencies" >&2
  echo "Review the effect boundary before updating this allowlist." >&2
  exit 1
fi
