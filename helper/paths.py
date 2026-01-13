from pathlib import Path
import os

PROJECT_ROOT = Path(os.getenv('PROJECT_ROOT', '/app'))

CACHE_DIR = PROJECT_ROOT / 'cache'
CACHE_DIR.mkdir(parents=True, exist_ok=True)

FLASK_STATIC_DIR = PROJECT_ROOT / 'Analysis' / 'time_series' / 'static' / 'images'
IMAGE_DIR = FLASK_STATIC_DIR
IMAGE_DIR.mkdir(parents=True, exist_ok=True)


def get_cache_path(filename):
    """Get path for a cache file"""
    return CACHE_DIR / filename


def get_image_path(filename):
    """Get path for an image file"""
    return IMAGE_DIR / filename