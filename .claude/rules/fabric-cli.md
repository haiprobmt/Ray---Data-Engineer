# Fabric CLI integration notes

Fabric CLI 1.7.0 resolves its configuration using the user profile at
`~/.config/fab/`. `FAB_CONFIG_DIR` does not relocate it. Ray uses a dedicated
child process profile for each project, avoiding the engineer's normal profile.
Fabric metadata authorization does not establish SQL endpoint authorization.
Acquire the SQL resource scope separately, and verify it with an actual bounded
SQL read. A Windows broker failure can be investigated using Ray's explicit
`login sql --browser` option, retaining the enrolled tenant and encrypted cache.
Never print raw authentication results or access tokens when diagnosing failures.
Fabric CLI 1.7.0 stores the SP identity and token cache without persisting the
client credential. A one-time SP login therefore does not establish unattended
renewal or authorization for another resource scope. Ray imports an explicitly
selected local dotenv file into project-bound Windows DPAPI storage, then loads
that credential only inside Fabric/SQL workers. Verify enrollment against a new
in-memory token cache so a cached access token cannot hide an invalid secret.
Fabric Notebook creation can reject nbformat-valid string cell sources with
InvalidNotebookContent; encode every cell source as a list of lines. Ray normalizes
SourceFile notebooks and their on-disk source before validation and fresh review.
Do not replay a failed creation blindly: inspect the operation and item inventory,
then use a corrected payload under a new reviewed action.
Fabric may enrich notebook metadata after creation. Inspect an exact-format
export and compare every code cell and binding before adopting any additions
into local run source. Obtain fresh validation and review; never ignore metadata
or weaken the definition match merely to allow a job to run.
An ipynb getDefinition response can omit definition.format. Preserve the explicit
format=ipynb selector in the adopted local definition; otherwise a subsequent
default-format export can differ despite unchanged notebook content.
