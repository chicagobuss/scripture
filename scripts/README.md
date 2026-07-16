# Lightweight transport harnesses

These dependency-free Python scripts exercise transport bytes during local and
fleet drills. They are deliberately **not** the Scripture client SDK and do not
define a stable application protocol.

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
