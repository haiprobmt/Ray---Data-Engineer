# ADR — Narrow item promotion before bulk publishing

Status: Accepted for the local POC, 2026-09-05.

The blueprint asks to evaluate/adopt fabric-cicd. We inspected the installed
fabric-cicd 1.3.0 API and its `FabricWorkspace` / `publish_all_items` entry points.
The library provides workspace publication, supported item scopes and deployment
parameterization. It is useful when a repository and workspace form a release unit.

Ray's current write contract approves an existing item ID, exact definition content,
source digest, policy and actor. We chose direct, narrow item-definition promotion
through the governed host rather than invoking the bulk publisher. This keeps the
reviewable action and audit receipt aligned with the actual API request. The choice
is an evaluated integration boundary, not a claim that fabric-cicd lacks filtering.

Adopting fabric-cicd later requires expanding all affected items/dependencies into a
reviewable plan, proving ID/environment mapping, binding the complete release to an
approval, handling partial publication and retaining source/rollback receipts. The
current package does not call `publish_all_items`, and no Fabric workspace was
published during development.

Transport decision: inspection of Fabric CLI 1.7.0 found implicit retries,
continuation traversal and LRO waiting in its API client. Ray uses its pinned
authentication adapter inside an isolated subprocess, then performs one urllib HTTP
request with redirects disabled. The host controls pagination, operation polling and
job polling. This relies on a version-pinned internal auth API; upgrade compatibility
must be checked before changing the Fabric CLI pin. HTTP credentials and raw error
bodies are not returned to the parent, model or Telegram.

References:

- [Microsoft fabric-cicd](https://github.com/microsoft/fabric-cicd)
- [fabric-cicd documentation](https://microsoft.github.io/fabric-cicd/)
- [Microsoft Fabric CLI](https://github.com/microsoft/fabric-cli)
