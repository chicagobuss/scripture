# SSH fleet exercise (product daemon)

```sh
./bin/build-release.sh
./bin/render-config.sh ./config/profiles/rustfs.env   # filled local profile
./bin/preflight.sh --inventory ./inventory/hosts.env
./bin/run-steady-state.sh --inventory ./inventory/hosts.env --run-id "$RUN_ID"
```

Uses rendered owner/standby YAML (no `REPLACE_WITH_*`). Plain `serve` only —
no seal-and-replace flag. R2 is **not** authorized while post-bootstrap
first-Serving remains an open decision.
