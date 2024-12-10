import imagehash
import pandas as pd
import sys
sys.path.append('../../')
from PIL import Image
from pathlib import Path


def build_media_path(row, data_dir):
    channel_name = row.channel_name
    media_id = row.media_id
    if row.msg_type == 'image':
        return f"{data_dir}/{channel_name}/images/{media_id}.jpg"
    elif row.msg_type == 'audio':
       return f"{data_dir}/{channel_name}/audio/{media_id}.png"


def hash_images(row):
    dhash, phash = None, None
    if row.msg_type == 'image':
        try:
            image = Image.open(row.file_path)
            dhash = imagehash.dhash(image)
            phash = imagehash.phash(image)
        except Exception:
            pass
    return str(dhash), str(phash)