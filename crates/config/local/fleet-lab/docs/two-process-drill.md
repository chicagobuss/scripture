# Two-process product drill (local)

```sh
scripture validate --config ./owner.yaml
scripture bootstrap --config ./owner.yaml --loglet-id gen-a0
scripture serve --config ./owner.yaml   # may be RecoveryRequired — open decision
scripture serve --config ./standby.yaml # Standby
```

No `--takeover-successor`. ha_claim=false.
