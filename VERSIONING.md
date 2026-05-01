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

### GitHub Release **title** (display name)

Use a **single pattern** so the Releases page matches tags and docs:

| Rule | Example |
|------|---------|
| Format | **`Pepi v` + the exact version string from the tag** (same characters as `refs/tags/...`). |
| Stable release | Tag `v2.2.5` → title **`Pepi v2.2.5`**. |
| Pre-release | Tag `v2.3.0-rc.1` → title **`Pepi v2.3.0-rc.1`**. |

**Do not** use mixed styles (`Pepi 2.2.5` without `v`, `Pepi v2.2.5` elsewhere, or raw `v2.2.5` alone)—that confuses readers comparing the tag, the release title, and `pepi/version.py`.

**CLI example** (edit an existing release):

```bash
gh release edit v2.2.5 --title "Pepi v2.2.5"
```

**New release** (tag must already exist):

```bash
gh release create v2.2.6 --title "Pepi v2.2.6" --generate-notes --latest
```

### GitHub Release **body** (Summary + Details)

The release **description** (markdown on GitHub) should follow one template so every version is skimmable the same way. Use these **top-level headings** in order; skip a section only when it does not apply, and write **“None.”** when there are no breaking changes or migrations.

| Section | Purpose |
|--------|---------|
| **Summary** | One short paragraph: who this release is for and the single main outcome (e.g. “Documentation and versioning policy; no API changes.”). |
| **Highlights** | Bullet list (about **3–8** items) of the most user-visible changes. Past tense, concrete nouns (CLI flag, tab name, endpoint path). |
| **Details** | Optional sub-headings under **Details** as needed—only include subsections that changed: **CLI**, **Web UI**, **HTTP API**, **Packaging / install**, **Docs**, **Performance / large logs**, **Security / privacy**. Short bullets or short paragraphs per subsection. |
| **Upgrade** | Exact steps: `pip install -U …`, `pepi --upgrade` (if applicable), or “pull `main` and reinstall editable”. Mention any required env or config changes. |
| **Breaking changes** | Bulleted list, or **None.** If something breaks old workflows or file formats, say how to detect and fix. |
| **Compatibility** | e.g. “Python ≥3.8”, “MongoDB JSON log line format unchanged”, “Browser UI still loads CDN assets”. |
| **Links** | Link to **compare** view on GitHub: prior release tag → this tag (e.g. `…/compare/v2.2.4…v2.2.5`). Optionally link to milestone or PR roll-up. |

**Pre-releases** (`-rc`, `-beta`, …): mark the GitHub release as **pre-release**; add a **Known issues** bullet list under **Details** (or **None.**).

**Example** (release notes body):

```markdown
## Summary

Documentation-only release: README and FAQ aligned with current CLI and API behavior; versioning policy added.

## Highlights

- Rewrote README sections for CLI, Web UI, sampling, cache, and API table.
- Expanded web FAQ for raw vs ingest and troubleshooting.
- Added `VERSIONING.md` and GitHub Release title convention.

## Details

### Docs

- New `VERSIONING.md` for tags and releases.

### CLI

- No behavior changes in this release.

## Upgrade

- Editable install: `git pull` and `pip install -e .` from your clone.

## Breaking changes

None.

## Compatibility

- Python ≥3.8 unchanged.

## Links

- Full diff: https://github.com/jenunes/pepi/compare/v2.2.4...v2.2.5
```

**Automation:** `gh release create vX.Y.Z --notes-file RELEASE_NOTES.md` keeps the body in a file for review in PR. You may run `--generate-notes` first, save the output, then wrap it in the sections above before publishing.

### Which versions get a GitHub Release

Not every git tag needs a Release; tags remain the full audit trail. **Releases** are for milestones users should notice (e.g. current line, major versions, last patch before a bump). Adjust the curated set over time; avoid publishing a Release for every historical patch unless you intend to maintain them all.

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
