---
name: delete-comment
description: Audit the code comments of the repository (or of the paths given as arguments). Sort each comment as Tautology, Excuse, Stale or Decision Record. Move decisions into ADRs, file issues for excuses, delete every comment, and tighten the no-comments ratchet. Use when the user runs /delete-comment, or asks to clean up, remove or audit comments.
---

# /delete-comment

The repository bans code comments (ADR `docs/adr/0001-decisions-live-in-adrs-not-comments.md`). This skill removes the
comments that are left and keeps their knowledge. `$ARGUMENTS` are optional git pathspecs (for example `src/SamoBot.Core`).
Without them, the whole repository is in scope.

The helper is `.claude/skills/delete-comment/comments.py`, run from the repository root with `python3`.

## 1. Inventory

```bash
python3 .claude/skills/delete-comment/comments.py estimate $ARGUMENTS
python3 .claude/skills/delete-comment/comments.py inventory $ARGUMENTS > /tmp/comments.txt   # or --json
```

Each block shows its id, file, line, text, and the line of code it sits on. Read every block. Do not sample.

## 2. Classify each block

| Category | Test | Action |
|---|---|---|
| Tautology | The name, type, signature or the next line already says it. A summary that rephrases the member name. Section dividers. | Delete |
| Stale | It names a type, member, file, number or process that no longer matches the code. Check with `grep`: comments are not compiled, so a `<see cref>` can point at nothing. | Delete |
| Excuse | It justifies a shortcut: TODO, "for now", "note: could be", "might want to", "UNVERIFIED", "before X existed" (a fallback for data that should be backfilled), "keeps its old name", a manual clean-up step, a lossy Down migration, a constant copied across languages. | Delete, and file a GitHub issue that describes the hack and the fix |
| Decision Record | It says *why*, which the code cannot show: a trade-off, a protocol or library quirk, an ordering constraint, a security property, how a number was chosen. | Move into an ADR, then delete |

When a block is both, the stronger action wins: Excuse beats Decision Record, and both beat Tautology. A Decision Record that
the README or an ADR already holds is a Tautology. Verify every number and name in a Decision Record against the code before you
copy it. Fix it in the ADR if it drifted, and say so.

Keep a table (id, file:line, category, target ADR or issue) in your notes. The final report needs the counts.

## 3. Act

1. **Decision Records → ADRs.** Add each one to the ADR of its topic in `docs/adr/` (see `docs/adr/README.md` for the list), as
   a bullet that names the type or member. Add a new numbered ADR only for a topic that has none, and add it to the index. Write
   in plain declarative sentences, not in "we should".
2. **Excuses → issues.** Group the excuses that share one fix into one issue. Search the open issues first, so you do not file a
   duplicate. Name the files and lines, the hack and the proposed fix. End the issue body with the footer that the environment
   prescribes for GitHub posts.
3. **Delete.** `python3 .claude/skills/delete-comment/comments.py strip $ARGUMENTS`. This removes every comment in scope except
   the tool directives (`/// <reference ...>`, `/** @type ... */`). It drops the lines that held only a comment and leaves no
   double blank lines.
4. **Fix what the deletion breaks.** An empty `catch` is an error (Sonar S108, ESLint `no-empty`). Do not put a comment back.
   Change the code instead: `ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing)` on an awaited `Task` that may be cancelled,
   a `try` helper that returns a value the caller uses, or a Debug log message for a cancellation path.

## 4. Tighten the ratchet

```bash
python3 .claude/skills/delete-comment/comments.py baseline
```

This rewrites `comments-baseline.txt` (C#, read by analyzer SB0001/SB0002) and prunes `src/frontend/eslint-suppressions.json`
(ESLint `local/no-comments`). The baselines may only shrink. `--allow-existing` adds comments to the baseline, and is only for
adopting the rule on new code that already has comments. Never use it to let a new comment in.

## 5. Verify

```bash
dotnet build SamoBot.sln -warnaserror
dotnet test src/SamoBot.Tests --no-build --filter "Category!=Integration"
(cd src/frontend && npm run lint && npm run typecheck && npm test)
```

## 6. Report

Report the counts and lines per category, the ADRs touched, the issues filed (with links), the code you changed to replace an
empty block, and the lines removed (`git diff --shortstat`). State whether the baselines are now empty.
