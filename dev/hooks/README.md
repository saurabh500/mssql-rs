# Git Hooks

This directory contains git hooks that can be shared across the team to maintain code quality and consistency.

## Setup

To install the git hooks for this project, run:

```bash
dev/setup-hooks.sh
```

## Available Hooks

### pre-commit

- **Purpose**: Ensures code is properly formatted before commits
- **What it does**: 
  - Runs `cargo fmt` to format Rust code
  - Prevents commit if formatting changes are needed
  - Shows which files need to be reviewed and re-staged

### Usage

The hook runs automatically before each commit. If formatting changes are needed:

1. The hook will show you which files were modified
2. Review the changes with `git diff`
3. Add the formatted files with `git add .`
4. Commit again

### Bypassing Hooks

If you need to bypass the pre-commit hook (not recommended), use:

```bash
git commit --no-verify
```
