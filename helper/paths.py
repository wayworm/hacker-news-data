from pathlib import Path
import os

PROJECT_ROOT = Path(os.getenv('PROJECT_ROOT', '/app'))

CACHE_DIR = PROJECT_ROOT / 'cache'
IMAGE_DIR = PROJECT_ROOT / 'static' / 'images'

CACHE_DIR.mkdir(parents=True, exist_ok=True)
IMAGE_DIR.mkdir(parents=True, exist_ok=True)


def get_cache_path(filename):
    """Get path for a cache file"""
    return CACHE_DIR / filename


def get_image_path(filename):
    """Get path for an image file"""
    return IMAGE_DIR / filename