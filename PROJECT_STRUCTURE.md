# Project structure

`src/` contains the typed Python domain and persistence code; `web/` contains the local interface; `scripts/` contains explicit maintenance and verification commands; `tests/` contains regression checks; `profiles/` declares the scope; and `data/profiles/` contains immutable manifests plus the bundled current snapshot. Historical binaries remain in their original release tags and are fetched only when explicitly requested.

Run `bash reproduce.sh` for the reviewer path. Generated databases, caches, and live-source workspaces are not source artifacts and are ignored by Git.
