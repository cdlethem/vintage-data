# Confined task executor protocol v2

The root-owned launcher supplies immutable admission JSON and a credential-free plain worktree. Modify only admitted paths, run only admitted verification argv, and return the exact protocol-v2 result. Never access parent paths, sockets, credentials, Airflow, Git remotes, or `.git`.
