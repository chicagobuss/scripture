# Lightweight transport harnesses

These dependency-free Python scripts exercise transport bytes during local and
fleet drills. They are deliberately **not** the Scripture client SDK and do not
define a stable application protocol.

`scribe-drill-preflight.sh` validates ignored multi-assignment SSH/ZeroTier
drill templates under `config/local/scribe-drills/` without printing secrets or
starting processes.

`submission-check.sh` captures the portable, deterministic evidence bundle for
the current source revision. It runs the core handoff/DataRef/spool suites, the
correctness campaign, and the Arrow/Parquet workload contract, then writes logs
and provenance beneath ignored `.tmp/submission-evidence/`:

```sh
./scripts/submission-check.sh
```

It intentionally makes no live deployment claim and never contacts object
storage, Kubernetes, or a package registry. A separate approved two-Scribe
object-store drill is required for live operational evidence.

`render-provider-scribe-drill.sh` creates non-secret, ignored two-Scribe YAML
for an isolated **real R2 or Amazon S3** run. It only renders configuration; it
does not contact a provider or copy credentials:

```sh
set -a; source ~/.config/scripture/r2.env; set +a
./scripts/render-provider-scribe-drill.sh \
  --backend r2 --run-id demo-001 \
  --a-host 10.244.231.86 --b-host 10.244.19.23
```

The later approved launch supplies credentials only in the Scribe process
environment. RustFS remains a disposable fault-injection backend, not the
default stand-in for real-provider evidence.

`scripture_send.py` targets the current provisional raw-lines TCP listener:

```sh
printf 'one\ntwo\n' | ./scripts/scripture_send.py --endpoint 127.0.0.1:9000 --inflight 8
```

Each newline-delimited input record receives one committed-only `OK` or `ERR`
response. It is suitable for piping Graphite/syslog-like fixtures or a file
from any host with Python 3.

`scripture_http.py` is an explicit HTTP request harness for a future HTTP
ingest decision. Scripture does **not** have an HTTP ingest contract today; use
an explicit URL only after that endpoint is introduced:

```sh
printf 'example' | ./scripts/scripture_http.py \
  --url http://127.0.0.1:8080/ingest \
  --content-type application/octet-stream
```

Neither script sends credentials on argv. The HTTP harness can read a bearer
token from `--bearer-env NAME` when a future endpoint defines bearer auth.
