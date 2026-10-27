# horsy-audit

See where your Claude Code tokens and money went in the last 30 days.

```
npx github:horsy-run/horsy-audit --plan max   # on a Pro/Max subscription
npx github:horsy-run/horsy-audit              # on the API
```

Needs Node 22+. It reads the session logs Claude Code keeps on your machine (`~/.claude/projects`) and prints a report. Nothing is uploaded, no account. Project names are hidden unless you pass `--show-names`.
