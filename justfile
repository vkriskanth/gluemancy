# List all available commands
default:
    @just --list

# ── Dependencies ────────────────────────────────────────────────────────────

# Install / sync all dependency groups
sync:
    uv sync --all-groups

# Add a runtime dependency  (usage: just add boto3)
add package:
    uv add {{package}}

# Add a dev-only dependency  (usage: just add-dev pytest)
add-dev package:
    uv add --group dev {{package}}

# Remove a dependency  (usage: just remove boto3)
remove package:
    uv remove {{package}}

# Upgrade all locked dependencies to their latest allowed versions
update:
    uv lock --upgrade
    uv sync --all-groups

# Show the resolved dependency tree
deps:
    uv tree

# Export pinned requirements files
export:
    uv export --no-dev --output-file requirements.txt
    uv export --output-file requirements-dev.txt
