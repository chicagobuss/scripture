# Local operator configuration

`config/local/` is intentionally ignored by Git. It is the local projection of
Joshua's private fleet/operator configuration: concrete hosts, kube contexts,
rendered manifests, credentials references, and run artifacts.

Tracker is the revisioned source of intent for that configuration:

- the `scripture` and `holylog-correctness-testing` folios record redacted
  campaign decisions and evidence;
- the `my-computers` folio records machine inventory and k0s topology;
- secret values are never stored in Tracker or Git.

Keep the committed repository surface generic. Promote a local finding only
when it becomes a reusable Scripture deployment contract, example, or tested
product capability.

Correctness-campaign orchestration lives under
`config/local/correctness-testing/` once created by the operator/runner.

## Multi-assignment Scribe drills

Local SSH/ZeroTier drill templates belong under
`config/local/scribe-drills/<run-id>/` (ignored). Start from the redacted
examples:

- `crates/scripture-cli/examples/scripture-multi-assignment.yaml`
- `crates/scripture-cli/examples/scripture-multi-assignment-standby.yaml`

Safe preflight (no secrets printed, does not start processes):

```bash
scripts/scribe-drill-preflight.sh
```

**No live SSH drill until deterministic evidence is green** (config/runtime
isolation tests and HA race proofs). Do not start RustFS traffic for this
package until that gate passes.

Drill shape (out of scope for Kubernetes / cloud object stores):

- dedicated RustFS on bignlittles only;
- serving/standby scribes on node-a and node-b via SSH + ZeroTier;
- separate producer host(s);
- generated/env-only credentials;
- unique run prefix, local evidence directory, and exact cleanup boundary.

Evidence must name the exact authority scope (`canon` + `verse`). Durable
store roots are Canon/Verse-derived (`{store.prefix}/cv/{hex(canon)}/{hex(verse)}`),
not assignment-id-derived — renaming an assignment id alone does not change
the root. Do not say “the scribe failed over” when only one Verse was
promoted — use targeted
`scripture promote --config multi.yaml --assignment ID --candidate-term N`
(standby is a dormant candidate until that promote).
