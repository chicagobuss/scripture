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
