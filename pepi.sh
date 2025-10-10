#!/bin/bash
# Pepi launcher script
PEPI_DIR="$HOME/.pepi"

# Store the original working directory
ORIGINAL_DIR="$(pwd)"

# Change to pepi directory to run the module
cd "$PEPI_DIR"

# Convert relative paths to absolute paths
ARGS=()
for arg in "$@"; do
    if [[ "$arg" =~ ^- ]]; then
        # It's an option, keep as is
        ARGS+=("$arg")
    elif [[ -f "$ORIGINAL_DIR/$arg" ]]; then
        # It's a file that exists in the original directory, make it absolute
        ARGS+=("$ORIGINAL_DIR/$arg")
    else
        # Keep the argument as is
        ARGS+=("$arg")
    fi
done

# Run pepi with processed arguments
python -m pepi "${ARGS[@]}"
