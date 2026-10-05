# Pre-Commit

Run all checks, fix any issues found, then present a summary table.

## Checks

### Rust checks

Run independent checks (`cargo check`, `cargo clippy`, `cargo fmt`, `cargo test`) in parallel.

- `cargo check`
- `cargo clippy`
- `cargo fmt --all`
- `cargo test`
- No `unwrap()` in non-test code without a `// safe:` comment
- No `unsafe` in non-test code without a `// SAFETY:` comment
- No unnecessary clones
- No deeply nested `use` (max one level of `{}` nesting)

### README

Check that `README.md` still matches the code and update it if the changes affect anything it describes.

- Actors listed match `src/system/`
- API endpoints and their JSON bodies match `src/system/api/`
- Settings mentioned match `settings.sample.yaml` and `src/settings.rs`
- Docker and Kubernetes instructions match `Dockerfile`, `kubernetes/` and `.github/workflows/ci.yml`
- Badges and links point to the current repository

## Code Review

After fixing all issues found in the checks, review the code changes for correctness, readability, and maintainability and propose improvements.
Summarize the findings according to severity.
