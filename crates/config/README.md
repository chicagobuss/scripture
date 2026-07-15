# Local operator configuration

`config/local/` is intentionally ignored by Git. It is the local projection of
Joshua's private fleet/operator configuration: concrete hosts, ZeroTier
addresses, rendered manifests, image-import helpers, credentials references,
and run artifacts.

Tracker is the revisioned source of intent for that configuration:

- the `scripture` folio records redacted environment/test configuration,
  operational decisions, and run evidence;
- the `my-computers` folio records machine inventory and k0s topology;
- secret values are never stored in Tracker or Git. They remain in local files,
  Kubernetes Secrets, or the process environment.

Keep the committed repository surface generic. Promote a local finding only
when it becomes a reusable Scripture deployment contract, example, or tested
product capability.

The current personal fleet harness is retained at
`config/local/fleet-lab/`. It is operator state, not a separate repository and
not part of the Scripture workspace. Its local Cargo workspace consumes the
adjacent Scripture checkout through path dependencies.
