#!/bin/bash
# Pepi launcher script
PEPI_DIR="$HOME/.pepi"
cd "$PEPI_DIR"
python -m pepi "$@"
