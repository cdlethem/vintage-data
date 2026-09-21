# Confined task executor protocol v2

The root-owned launcher supplies immutable admission JSON and a credential-free
plain worktree. Deliver the smallest complete implementation within the admitted
paths. The trusted worker runs the admitted checks, gives one bounded repair pass
when a check fails, and retains both observations. Repair failures caused by the
change without weakening checks or expanding scope. Return the exact protocol-v2
result. Never access parent paths, sockets, credentials, Airflow, Git remotes, or `.git`.
