# Gold preparation timeout — 6 September 2026

The Gold task stopped during local code preparation, before any Gold cloud action was proposed or submitted. The task and its three draft files were preserved. It was a separate task from the successful Silver deployment.

The saved timeline shows a model turn beginning at 13:01:47 UTC and being stopped at 13:11:48 UTC. The model had written the notebook, definition and tests; six local tests and source validation passed at 13:08:48 UTC. It emitted a further progress message at 13:09:03 UTC, but no final response was received before the fixed 600-second deadline. The parent process treated that deadline as a total limit even for active engineering work. The technical Telegram timeout message came from the service version running before the earlier reload.

## Repair

- Engineering model turns now use the configured timeout as an inactivity limit. While the official SDK reports activity, a turn can continue up to three times that duration, capped at two hours. With this project's existing setting, those limits are ten minutes without activity and thirty minutes overall. Project policy/configuration bindings and Fabric job timeouts are unchanged.
- The worker consumes the official SDK's public notification stream. Its activity file contains only a sequence number and an allowlisted activity category. It never stores reasoning, command arguments, source text or credentials in the progress record. Repeated checkpoints, unrelated service notifications and malformed records do not extend the deadline.
- The worker still requires a completed turn and a valid final answer. Commentary, incomplete streams and failed/interrupted turns cannot become successful results.
- `/status` can show whether Ray is preparing the response, updating files or running a local command. Errors distinguish inactivity from the maximum duration and preserve whether the failure happened during authoring, review or local validation. Exact timing remains in `/details technical`.
- Stop/cancellation, saved source checkpoints, fresh independent review, cloud policy and action receipts remain enforced. A timeout never retries a cloud mutation.

## Verification

The original parent-loop regression failed with `Codex turn timed out` while a simulated worker continued reporting activity. It passes with the repair. Separate tests retain the silent-worker cutoff, maximum duration, cancellation, final-answer rules, review-stage reporting and safe progress handling under a Windows file-sharing conflict.

A real read-only Codex run against the preserved Gold repository reran six source-contract tests and validated 34 source files. Both commands exited zero. The repository manifest was unchanged, and the parent observed real SDK activity categories. This verifies the streaming integration and local draft checks; it does not claim a Gold deployment or a live model run longer than ten minutes.

The full suite passed: **350 tests in 140.21 seconds**. The focused timeout/stream suite also passed all 25 cases, including three added checks that a chat timeout does not tell the user to resume an unrelated engineering task. `pip check` and `git diff --check` passed. The idle Telegram gateway was reloaded with the repair; the original Gold task remains paused with its source checkpoint and no Gold cloud action receipts.
