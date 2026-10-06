from __future__ import annotations

from typing import BinaryIO

import imagehash
from blake3 import blake3
from PIL import Image


def compute_blake3(stream: BinaryIO, chunk_size: int = 1024 * 1024) -> str:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    digest = blake3()
    while chunk := stream.read(chunk_size):
        digest.update(chunk)
    return digest.hexdigest()


def compute_phash(image: Image.Image, hash_size: int = 8) -> str:
    if hash_size <= 0:
        raise ValueError("hash_size must be positive")
    return str(imagehash.phash(image, hash_size=hash_size))

