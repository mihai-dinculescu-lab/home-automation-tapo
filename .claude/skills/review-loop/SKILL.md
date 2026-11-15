---
name: review-loop
description: Run /code-review repeatedly on a target, fixing only findings that pass the review bar below, until three consecutive rounds accept nothing. Ends with /simplify and a summary for review. Does not commit.
---

# Review Loop

Review a target until the reviews stop producing findings worth fixing, without letting the fixes complicate the code, the settings or the published payloads.

The argument is the target, passed through to `/code-review` (a path, branch or PR number). With no argument, review the current diff, and name the untracked files to the reviewer, since a diff does not show them. Never stage or unstage anything: the fixes stay unstaged on top of the user's work.

## Each round

1. **Review.** Run `/code-review high <target>`, never with `--fix`. Tell the reviewer the reject list below, and to leave `settings.yaml` and the devices alone. From the second round on, include the loop's own fixes and tell it what this run has already rejected.
2. **Triage.** Judge every finding against the review bar, checking the claimed failure against the code. Do not accept a finding on the reviewer's word. A repeat of a rejected finding, or of one the user declined before (recorded in memory), is rejected again. When unsure whether a failure is reachable, reject it as "unconfirmed".
3. **Fix.** Apply the accepted findings only, each with the smallest change that works, and carry it through the README, `settings.sample.yaml` and the tests where they describe it. Never fix a disagreement by weakening a safety property in the README; when the code fix costs too much, raise it with the user instead. If a fix needs more code than the problem deserves, edit it back out and reject the finding. Never undo with git: the same files hold the user's work.
4. **Check.** After a round with fixes, run `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings` and `cargo test`.

## Review bar

This is a deployed service with one operator, not a library. Accept a finding only if all of these hold:

- **It is real.** It can be reached with settings that pass validation and with device, broker or clock behavior that actually occurs.
- **It matters.** A heater left on or off against the rules, a safety property in the README that no longer holds, a panic or a stuck actor, a settings mistake read silently instead of failing at startup, a published value or span field that says something false, or a README or sample that disagrees with the code.
- **The fix costs less than the problem.** Every extra setting, validation rule, `Reason` and payload field is paid for in the settings file, the README and whatever consumes the decisions topic.

A thermostat finding is checked against the safety properties in the README. Unknown input deciding On, or configuration that can silently disable a safeguard, is accepted on a thinner case than anything else.

Reject, whatever the reviewer's confidence:

- The case of no OTLP exporter. An error recorded with `record_error!` counts as observed.
- A log line next to a `record_error!`.
- Items that are `pub` "only for tests".
- Blank values in `settings.sample.yaml`.
- Validation that paho or tapo already does.
- Dropping a section's `enabled` flag, or giving a safety mechanism one.
- Conflicts with "another controller": the thermostat is the only thing that drives its heater plugs.
- Work that a plan under `docs/plans/` schedules for later or lists as out of scope.
- Safety properties the README already marks as not met.
- New test approaches, such as mocked devices.
- Style, and "could be more defensive".

## Claims about devices

A claim about what a device sends is not settled by reasoning. Check the pinned `tapo` source for what the library parses, and ask a real device through the tapo MCP when that is not enough. The devices run a lived-in home and the production service uses them:

- Read-only calls only, one per question: even a read logs in, which can fail a request the service has in flight on that device. Never switch a plug, take a snapshot or publish to the broker.
- Never run the service locally as a probe, and never read `settings.yaml`, which holds the credentials.
- Never repeat a login the device rejected: it locks itself after too many, for the production service too. Stop and tell the user.

If a claim cannot be settled this way, reject it as "unconfirmed".

## Stopping

Stop when three consecutive rounds accepted nothing, or after 24 rounds. Only accepted findings count; a reviewer can always produce something.

Then:

1. Run `/simplify` on the loop's fixes, if there were any, and the checks again.
2. Summarise in chat: rounds run and why the loop stopped, a table of applied fixes (file, problem, change, evidence), a table of rejected findings with a one-line reason each, and any device calls made.
3. Do not commit or stage. Wait for the user to review.
