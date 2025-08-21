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

## Running Tools and Tests with `uv`

To maintain a consistent and isolated environment, always use `uv run` to execute tools and tests. This ensures that the correct versions of packages specified in the project's virtual environment are used, without relying on the system's `PATH`.

### Example: Installing and Running `pytest`

If you need to run tests, follow this two-step process:

1.  **Install `pytest` and any other dev dependencies into the virtual environment:**
    ```bash
    uv pip install pytest pytest-mock
    ```

2.  **Run the tests using `uv run`:**
    ```bash
    uv run pytest
    ```

### General Usage

For any package or tool, first ensure it's installed with `uv pip install <package-name>`, then execute it with `uv run <command>`. This avoids issues with shell paths and ensures you are using the project-specific dependencies.