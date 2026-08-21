# `.claude/` — agent configuration

This directory configures [Claude Code](https://claude.com/claude-code) for this
repository. It is committed so the whole team gets the same behaviour.

| Path | Purpose |
|---|---|
| `settings.json` | Shared permissions: pre-approved read/test/lint commands, `ask` for anything that leaves the machine, `deny` for destructive or publishing commands. |
| `commands/*.md` | Slash commands (`/test`, `/lint`, `/typecheck`, `/review-pr`, `/add-tests`, `/commit`, `/docs`, `/release`). |
| `rules/*.md` | Coding, testing and commit conventions. Imported into context by `CLAUDE.md` via `@` references. |

Personal overrides belong in `.claude/settings.local.json`, which is gitignored.

## Optional: format on every edit

If you want ruff to format automatically after Claude edits a file, add a
`PostToolUse` hook to `.claude/settings.local.json` (keeping it out of the shared
config until you have confirmed it behaves the way you expect):

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Edit|Write",
        "hooks": [
          { "type": "command", "command": "uv run ruff format . >/dev/null 2>&1; exit 0" }
        ]
      }
    ]
  }
}
```

Verify the hook schema against your installed Claude Code version (`/hooks`)
before relying on it — the format has changed between releases, and a malformed
hook fails quietly. This is deliberately not enabled by default: the `pre-commit`
hook already formats everything before it can reach a commit.
