import aiohttp
import asyncio
from src.log import setup_logger

log = setup_logger(__name__)

async def upload_to_catbox(file_url: str) -> str | None:
    """
    Downloads an image from a URL and uploads it to Catbox.moe.
    Returns the Catbox URL if successful, otherwise None.
    """
    try:
        async with aiohttp.ClientSession() as session:
            # Download the image
            async with session.get(file_url) as resp:
                if resp.status != 200:
                    log.error(f"Failed to download image from {file_url}: Status {resp.status}")
                    return None
                file_data = await resp.read()

            # Upload to Catbox
            data = aiohttp.FormData()
            data.add_field('reqtype', 'fileupload')
            data.add_field('userhash', '') # Optional, leave empty for anonymous
            
            # Extract filename from URL or use a default
            filename = file_url.split('/')[-1].split('?')[0] or 'image.jpg'
            
            data.add_field('fileToUpload', file_data, filename=filename)

            async with session.post('https://catbox.moe/user/api.php', data=data) as resp:
                if resp.status == 200:
                    catbox_url = await resp.text()
                    return catbox_url.strip()
                else:
                    log.error(f"Failed to upload to Catbox: Status {resp.status}")
                    return None

    except Exception as e:
        log.error(f"Error mirroring image to Catbox: {e}")
        return None
