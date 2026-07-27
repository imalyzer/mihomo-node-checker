# mihomo-node-checker

Automated pipeline that harvests free Clash/Mihomo proxy lists, keeps a
**sticky pool** across runs, and publishes dual providers:

- [`output/stable-nodes.yaml`](output/stable-nodes.yaml) — nodes with streak ≥ 3
- [`output/fresh-nodes.yaml`](output/fresh-nodes.yaml) — newer / lower-streak nodes
- [`output/backup-nodes.yaml`](output/backup-nodes.yaml) — union of both

## Live proxy-providers

```text
https://raw.githubusercontent.com/imalyzer/mihomo-node-checker/main/output/stable-nodes.yaml
https://raw.githubusercontent.com/imalyzer/mihomo-node-checker/main/output/fresh-nodes.yaml
```

Schedule: every hour (`0 * * * *`) plus `workflow_dispatch`.
