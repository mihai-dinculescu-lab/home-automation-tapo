---
name: release
description: Release a new version by bumping the crate version and pushing a vX.Y.Z tag, which makes CI publish the semver-tagged Docker images
---

# Release

Bump the version, tag it and push the tag so that CI publishes the versioned Docker images to `ghcr.io/mihai-dinculescu-lab/home-automation-tapo`.

The version can be passed as an argument (`/release 0.3.0`, or `major` / `minor` / `patch`). The tag is always `v<version>`.

## Steps

### Preconditions

Stop and report if any of these fail.

- The current branch is `main` and the working tree is clean
- `git fetch origin --tags` shows `main` is not behind `origin/main`
- The latest CI run on `main` succeeded (`gh run list --workflow ci.yml --branch main --limit 1`)

### Pick the version

- Find the latest tag (`git describe --tags --abbrev=0`) and list the commits since it (`git log <tag>..HEAD --oneline`)
- If no version was passed, propose one from those commits (breaking change → major, `feat` → minor, otherwise patch; while on `0.x` a breaking change bumps the minor) and confirm it with the user
- The tag must not already exist locally or on `origin`

### Bump and commit

- Set `version` in `Cargo.toml`, then run `cargo check` so that `Cargo.lock` picks it up
- Commit both files with `/commit`, using the message `chore(release): v<version>`

### Tag and push

- Create an annotated tag on the release commit: `git tag -a v<version> -m "v<version>"`
- Always confirm with the user before pushing
- Push the branch and the tag together: `git push origin main v<version>`

This starts two CI runs: the `main` one publishes `main` and deploys as usual, the tag one publishes `<version>`, `<major>.<minor>`, `<major>` and `latest`.

### Verify

- Watch the tag run until it finishes (`gh run list --workflow ci.yml --branch v<version> --limit 1`, then `gh run watch <id> --exit-status`)
- Check that the version tags are in the registry:

```bash
TOKEN=$(curl -s "https://ghcr.io/token?scope=repository:mihai-dinculescu-lab/home-automation-tapo:pull" | jq -r .token)
curl -s -H "Authorization: Bearer $TOKEN" "https://ghcr.io/v2/mihai-dinculescu-lab/home-automation-tapo/tags/list?n=1000" | jq -r '.tags[]' | grep -v '^sha-'
```

- Report the published tags, or the failing job and its logs if the run failed
