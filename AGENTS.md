# Agent Guidelines and Project Conventions

This document contains specific guidelines and conventions for agents working on this project.

## Dependency Management with `uv`

When installing dependencies from the `uv.lock` file, please prefer the `uv sync` command over `uv pip sync`.

```bash
# Preferred command
uv sync

# Avoid this command
uv pip sync uv.lock
```

### Rationale

-   **Idiomatic Usage:** `uv sync` is the high-level, idiomatic command designed specifically for synchronizing a virtual environment with a project's lock file.
-   **Clarity and Simplicity:** It's a more direct and expressive command that clearly communicates the intent of making the environment match the lock file.
-   **Future-Proofing:** While `uv pip sync` works, the top-level `uv sync` is the recommended path and may receive more features or optimizations in the future.

Sticking to `uv sync` ensures consistency across the project.