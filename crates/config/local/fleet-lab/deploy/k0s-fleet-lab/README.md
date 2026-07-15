# k0s local overlay (product daemon)

Uses Scripture product manifests under `../../../../deploy/kubernetes/`:

```sh
./bin/apply-baseline.sh
```

Requires personal Secret/ConfigMap fill and image import (`./bin/import-image.sh`).

Deleted (broken / lab-node era): `apply-load.sh`, `crash-to-recovery.sh`,
`owner-bootstrap.yaml`, `owner-recovery.yaml`, `load-jobs.yaml`.

Post-bootstrap first Serving is an **open product decision** — do not authorize
R2 until that lands. Owner may observe `RecoveryRequired` after the Job.
