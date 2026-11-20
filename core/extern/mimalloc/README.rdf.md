The framework embeds upstream mimalloc v3.2.8 (commit
7b7fca4b7013d77e356d96e055fec309cb3cbbd6) in native Linux executables.
The include and src directories and MIT license are copied from that release.
The build uses the upstream single-object allocation override in src/static.c.
