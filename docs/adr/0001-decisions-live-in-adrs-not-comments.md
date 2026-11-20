# 0001. Decisions live in ADRs, not in code comments

Status: Accepted (2026-11)

## Context

An audit of every comment in the repository found 759 comment blocks (1,230 lines), not counting the vendored MSW worker:

| Kind | Blocks | Lines | Action |
|---|---|---|---|
| Tautology: repeats what the code already says | 419 | 457 | deleted |
| Decision Record: an important, non-obvious decision | 303 | 701 | moved into ADRs 0002 to 0015 |
| Excuse: lets a hacky implementation stand | 22 | 49 | deleted, and an issue filed for each hack |
| Stale: describes code that no longer exists | 13 | 21 | deleted |
| Tool directive (`/// <reference>`, `/** @type */`) | 2 | 2 | kept |

The audit, one row per comment, with where each one went, is in [0001-comment-audit.csv](0001-comment-audit.csv) (commit
`fec315c` is the state before the audit). The excuses became issues #41 to #48.

Comments drift from the code. A comment that excuses a shortcut lets the next change copy it. Over half of the comments said
only what the names and types already say.

## Decision

- Source files (C#, TypeScript, JavaScript) have no comments. Names, types and tests carry the meaning.
- A decision that the code cannot show goes into an ADR in this folder. Each ADR names the types and members it is about.
- A shortcut becomes a GitHub issue, not a comment.
- Two tool directives stay, because tools read them: `/// <reference types="vite/client" />` in `vite-env.d.ts` and the
  `/** @type {import('tailwindcss').Config} */` annotation in `tailwind.config.js`.
- The build enforces this with a ratchet. A file may not gain comments, and when a file loses comments its baseline must
  shrink in the same change:
  - C#: analyzer `SB0001` (`src/SamoBot.Analyzers`). The baseline is `comments-baseline.txt`.
  - TypeScript: ESLint rule `local/no-comments` (`src/frontend/eslint.config.js`). The baseline is ESLint's
    `src/frontend/eslint-suppressions.json`.
- The `/delete-comment` skill (`.claude/skills/delete-comment`) repeats the audit.

## Consequences

- XML documentation no longer shows in IDE tooltips. Read the ADR for the reasons and the tests for the behaviour.
- A pull request that adds a comment fails the build. Put the reason in an ADR, or in the commit message when it is about
  the change and not about the code.
