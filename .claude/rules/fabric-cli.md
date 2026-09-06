# Fabric CLI integration notes

Fabric CLI 1.7.0 resolves its configuration using the user profile at
`~/.config/fab/`. `FAB_CONFIG_DIR` does not relocate it. Ray uses a dedicated
child process profile for each project, avoiding the engineer's normal profile.
No cloud credentials or real workspace IDs were supplied during implementation.
