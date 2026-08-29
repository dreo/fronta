# Security

## Reporting

Report vulnerabilities privately to the maintainer listed in `pyproject.toml`; do not open a
public issue. You will get an acknowledgement within a few days and a fix or a mitigation before
any public disclosure.

## Boundaries

- **Database.** Fronta needs a role that can create the `fronta` schema (`fronta db init`) and read
  and write its tables. Task inputs, results, progress and error metadata are stored as JSONB in
  the application's database: treat them like any other application data (encryption at rest,
  backups, access control). Execution tokens never leave the worker and the database.
- **Server.** `fronta server` binds `127.0.0.1` by default and is meant for a trusted network or
  a reverse proxy that terminates TLS. `FRONTA_SERVER_TOKEN` is required (the server refuses to start without it, so a browser on
  the same host cannot be made to enqueue or cancel tasks) and is sent as `Authorization: Bearer`; the dashboard is public HTML that asks for the token and keeps it in
  the browser's local storage. Request bodies are capped before parsing.
- **Sandbox.** Process tasks run in bubblewrap with new user, PID, mount, network and IPC
  namespaces, read-only allowlisted binds, private bounded tmpfs, no network, a cleared
  environment and per-definition rlimits. The application chooses the executable; only its
  JSON input comes from outside. The sandbox contains hostile input, not hostile executables:
  a malicious binary can still burn CPU and memory within its rlimits, and the kernel attack
  surface of unprivileged user namespaces is the host's to manage (seccomp/AppArmor policies,
  kernel updates). CPU and memory rlimits are per process.
- **Handlers.** asyncio handlers run in the worker process with the worker's privileges and
  database access; they are trusted code.
