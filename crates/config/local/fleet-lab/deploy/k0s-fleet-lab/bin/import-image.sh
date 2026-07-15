#!/usr/bin/env bash
# Import product scripture image onto a k0s node (personal).
set -euo pipefail
IMAGE="${1:-scripture:0.1.0}"
NODE="${2:?usage: $0 IMAGE NODE}"
echo "import $IMAGE onto $NODE (product image; no fleet-lab-node)"
# Operator fills: docker save | ssh ctr -a /run/k0s/containerd.sock images import -
echo "see Scripture deploy/kubernetes/Dockerfile for the image build"
