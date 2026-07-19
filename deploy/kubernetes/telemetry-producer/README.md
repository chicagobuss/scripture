# Phase 2 packaging for the first Kubernetes telemetry producer.
#
# Namespace: scripture-prototype (never scripture-lab / Tracker RustFS).
# Live drill uses a lab raw-lines sink until product Scribe + dedicated RustFS
# stand; that sink is not Canon/Oracle and must not be claimed as HA history.
#
# Apply order:
#   kubectl apply -f namespace.yaml
#   kubectl apply -f serviceaccount.yaml -f networkpolicy.yaml
#   kubectl apply -f node-exporter.yaml -f kube-state-metrics.yaml
#   kubectl apply -f egress-preflight-job.yaml   # wait for success
#   # build + import scripture-telemetry-producer:0.1.0 onto the target node
#   kubectl apply -f lab-sink.yaml -f producer.yaml
#
# Image build (no SSH; crate has no Holylog dep):
#   DOCKER_BUILDKIT=1 docker build \
#     -f deploy/kubernetes/telemetry-producer/Dockerfile \
#     -t scripture-telemetry-producer:0.1.0 .
