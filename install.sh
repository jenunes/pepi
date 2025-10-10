#!/bin/bash
# Pepi Installation Script
# Installs Pepi to ~/.pepi/

INSTALL_DIR="$HOME/.pepi"
REPO_URL="https://github.com/jenunes/pepi.git"

echo "🚀 Installing Pepi to $INSTALL_DIR..."

# Clone or update repository
if [ -d "$INSTALL_DIR" ]; then
    echo "Pepi already installed, updating..."
    cd "$INSTALL_DIR"
    git pull origin main
else
    git clone "$REPO_URL" "$INSTALL_DIR"
fi

# Install dependencies
cd "$INSTALL_DIR"
pip install -r requirements.txt

# Create symlink in PATH
mkdir -p "$HOME/.local/bin"
ln -sf "$INSTALL_DIR/pepi.sh" "$HOME/.local/bin/pepi"

echo "✅ Pepi installed successfully!"
echo "Run 'pepi --help' to get started"
