# 420wedddata

## Path validation
The uploader verifies required paths during startup:

- If `tdl_path` does not exist, a critical error is logged and the program exits.
- Missing directories listed in `watch_folders` are created automatically and the action is logged.

