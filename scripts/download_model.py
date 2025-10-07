#!/usr/bin/env python3
"""
Download tiny GGUF model for embedded index recommendations.
This script downloads a ~350MB quantized model optimized for fast CPU inference.
"""

import os
import sys
import urllib.request
from pathlib import Path


# Tiny model options (sorted by size, smallest first)
MODELS = {
    "qwen2.5-0.5b": {
        "url": "https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/resolve/main/qwen2.5-0.5b-instruct-q4_k_m.gguf",
        "size": "352 MB",
        "description": "Smallest, fastest - Good for quick index tips",
    },
    "tinyllama-1.1b": {
        "url": "https://huggingface.co/TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/main/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf",
        "size": "669 MB",
        "description": "Small, balanced - Better recommendations",
    },
    "phi3-mini": {
        "url": "https://huggingface.co/microsoft/Phi-3-mini-4k-instruct-gguf/resolve/main/Phi-3-mini-4k-instruct-q4.gguf",
        "size": "2.3 GB",
        "description": "Best quality - Excellent recommendations (larger)",
    },
}


def download_model(model_name: str, target_dir: Path):
    """Download model from HuggingFace."""
    
    if model_name not in MODELS:
        print(f"❌ Unknown model: {model_name}")
        print(f"Available models: {', '.join(MODELS.keys())}")
        sys.exit(1)
    
    model_info = MODELS[model_name]
    url = model_info['url']
    filename = url.split('/')[-1]
    target_path = target_dir / filename
    
    # Check if already exists
    if target_path.exists():
        print(f"✅ Model already exists: {target_path}")
        return target_path
    
    # Create directory
    target_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📥 Downloading {model_name} ({model_info['size']})...")
    print(f"   {model_info['description']}")
    print(f"   From: {url}")
    print(f"   To: {target_path}")
    print()
    
    # Download with progress
    def progress(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            percent = min(downloaded * 100.0 / total_size, 100)
            mb_downloaded = downloaded / (1024 * 1024)
            mb_total = total_size / (1024 * 1024)
            print(f"\r   Progress: {percent:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end='')
    
    try:
        urllib.request.urlretrieve(url, target_path, progress)
        print()  # New line after progress
        print(f"✅ Download complete: {target_path}")
        return target_path
    
    except Exception as e:
        print(f"\n❌ Download failed: {e}")
        if target_path.exists():
            target_path.unlink()  # Remove partial file
        sys.exit(1)


def main():
    """Main download script."""
    
    # Get package directory
    script_dir = Path(__file__).parent
    package_dir = script_dir.parent / "pepi"
    models_dir = package_dir / "models"
    
    print("🤖 Pepi Index Advisor - Model Downloader")
    print("=" * 50)
    print()
    
    # Check if model already exists
    if models_dir.exists():
        existing_models = list(models_dir.glob("*.gguf"))
        if existing_models:
            print(f"ℹ️  Found existing model: {existing_models[0].name}")
            response = input("Download a different model? (y/n): ").strip().lower()
            if response != 'y':
                print("✅ Using existing model")
                return
            print()
    
    # Show available models
    print("Available models:")
    for i, (name, info) in enumerate(MODELS.items(), 1):
        print(f"  {i}. {name:20} - {info['size']:8} - {info['description']}")
    print()
    
    # Get user choice
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = input(f"Select model (1-{len(MODELS)}) or name [default: 1]: ").strip()
        if not choice:
            choice = "1"
    
    # Parse choice
    if choice.isdigit():
        idx = int(choice) - 1
        if 0 <= idx < len(MODELS):
            model_name = list(MODELS.keys())[idx]
        else:
            print(f"❌ Invalid choice: {choice}")
            sys.exit(1)
    elif choice in MODELS:
        model_name = choice
    else:
        print(f"❌ Invalid model: {choice}")
        sys.exit(1)
    
    print()
    download_model(model_name, models_dir)
    
    print()
    print("✅ Setup complete!")
    print()
    print("💡 The model will be automatically loaded by pepi.")
    print("   No additional configuration needed.")
    print()
    print("🚀 Try it:")
    print("   pepi --fetch logfile.log --queries")
    print("   # Or in web UI: Click 'Get Index Recommendations' button")


if __name__ == "__main__":
    main()

