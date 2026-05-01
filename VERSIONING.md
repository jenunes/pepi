# Versioning and git tags (Pepi)

This document defines how we version Pepi and name git tags. **New tags follow this policy.** Older tags remain for history and reproducibility.

## Tag format (new work)

- **Released versions:** `vMAJOR.MINOR.PATCH` — [Semantic Versioning 2.0.0](https://semver.org/) with a leading `v` (e.g. `v2.2.6`).
- **Pre-releases:** `vMAJOR.MINOR.PATCH-<prerelease>.N` using ASCII identifiers, e.g.:
  - `v2.3.0-rc.1`
  - `v2.3.0-beta.2`
  - `v2.3.0-alpha.1`
- **Prefix:** always lowercase `v` immediately followed by the version (matches existing repo tags).

## Legacy tags (do not rename in bulk)

The repository already contains tags that predate this policy:

| Pattern | Examples | Note |
|--------|----------|------|
| Four numeric segments | `v0.0.1.1` … `v0.0.1.9`, `v0.0.2.1` … | Not strict three-part SemVer; kept as-is. |
| Phase milestones | *(formerly `v2.1.0-phase1` … `phase8`)* | Those tags were **removed from `origin`** (2026-05); commits remain in history. **Do not re-add** `*-phaseN` tags on the remote. |

Some historical tags are **lightweight** (no tag message). New release tags should be **annotated**.

## When to create a tag

1. The release commit is chosen on **`main`** (or the agreed release branch after merge).
2. **`pepi/version.py`** (and any user-visible version strings in the web UI / `package.json` when used) match that release.
3. Create an **annotated** tag on that commit:

   ```bash
   git tag -a vX.Y.Z -m "Release vX.Y.Z: concise summary"
   git push origin vX.Y.Z
   ```

4. Publish a **GitHub Release** from that tag (web UI or `gh release create vX.Y.Z ...`).

Patch gaps (e.g. no `v2.2.2`) are acceptable if no release was shipped.

## Examples

| Valid | Invalid (for new tags) |
|-------|-------------------------|
| `v2.2.6` | `2.2.6` (missing `v`) |
| `v3.0.0-rc.1` | `v3.0.0-rc` (prerelease needs a dot and number) |
| `v2.4.0-beta.1` | `v2.4.0beta1` |
| `v2.5.0` | `v2.5.0.1` (use PATCH bump instead of a fourth segment) |

## Hotfixes

Ship as the next **PATCH** tag (`v2.2.7`) from the hotfix branch merged to `main`. Avoid ad-hoc suffixes unless you intentionally use SemVer build metadata (discuss with maintainers first).

## GitHub Releases vs git tags

- A **git tag** marks a commit; pushing it is not enough for the Releases page “Latest” badge.
- A **GitHub Release** is created separately and should accompany user-facing versions.

## CI and automation

- Workflows may trigger on `push` of tags matching `v*.*.*` (tighten patterns if you need to ignore prereleases).
- Any script that lists “latest” tags should use **SemVer ordering** (e.g. `sort -V`, or a proper semver library), not lexical sort alone.
- `install.sh` / `pepi --upgrade` paths that read GitHub tags should remain compatible with `vMAJOR.MINOR.PATCH` and optional `-beta.N` / `-rc.N` suffixes.

## Changing or removing a tag (break glass)

Renaming or deleting a tag that others may have fetched **breaks reproducible installs**. Prefer issuing a **new** correct tag on the intended commit. If you must delete a remote tag:

```bash
git push origin :refs/tags/vX.Y.Z
```

Then coordinate with anyone who pinned that tag.
