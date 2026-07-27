# mihomo-node-checker

Automated pipeline that harvests free Clash/Mihomo proxy lists, filters them with
[`clash-speedtest`](https://github.com/faceair/clash-speedtest), then probes
representative Group-A domains via Mihomo's `/proxies/{name}/delay` API.

## Live proxy-provider

After the GitHub Actions workflow succeeds, use:

```text
https://raw.githubusercontent.com/<owner>/mihomo-node-checker/main/output/backup-nodes.yaml
```

## Local regenerate of domain targets

```bash
python scripts/extract_targets.py --config /path/to/Master-Config.yaml --output data/group-a-targets.txt
```

Do **not** commit `Master-Config.yaml` into this repo.

## Schedule

GitHub Actions runs every 2 hours (`0 */2 * * *`) and on `workflow_dispatch`.
