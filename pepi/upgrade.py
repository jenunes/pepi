"""Version check and self-upgrade functionality for Pepi."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Optional

import click
import requests
from packaging import version as pkg_version

from pepi.version import __version__


def check_for_updates() -> Optional[str]:
    """Check GitHub for newer version."""
    try:
        response = requests.get(
            "https://api.github.com/repos/jenunes/pepi/tags",
            timeout=5
        )
        if response.status_code == 200:
            tags = response.json()
            if tags:
                latest_version = tags[0]['name'].lstrip('v')
                current_version = __version__

                if pkg_version.parse(latest_version) > pkg_version.parse(current_version):
                    return latest_version
    except (requests.RequestException, ValueError, KeyError, IndexError):
        pass
    return None


def perform_upgrade() -> None:
    """Upgrade Pepi to latest version."""
    install_dir = Path.home() / '.pepi'

    if not install_dir.exists():
        click.echo("❌ Pepi not installed in ~/.pepi/")
        click.echo("Please reinstall using the installation script")
        return

    click.echo("🔍 Checking for updates...")

    latest_version = check_for_updates()
    if not latest_version:
        click.echo(f"✅ Already up to date (v{__version__})")
        return

    click.echo(f"📦 New version available: v{latest_version}")
    click.echo(f"   Current version: v{__version__}")

    if click.confirm("Upgrade now?"):
        click.echo("⬇️  Downloading latest version...")

        try:
            subprocess.run(
                ["git", "pull", "origin", "main"],
                cwd=install_dir,
                check=True,
                capture_output=True
            )

            click.echo("📦 Updating dependencies...")
            subprocess.run(
                ["pip", "install", "-r", "requirements.txt"],
                cwd=install_dir,
                check=True,
                capture_output=True
            )

            click.echo(f"✅ Successfully upgraded to v{latest_version}")
            click.echo("   Restart pepi to use the new version")
        except subprocess.CalledProcessError as e:
            click.echo(f"❌ Upgrade failed: {e}")


def check_version_async() -> None:
    """Background check for updates."""
    latest = check_for_updates()
    if latest:
        click.echo(f"💡 New version available: v{latest} (current: v{__version__})")
        click.echo("   Run 'pepi --upgrade' to update")
