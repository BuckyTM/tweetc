import aiohttp
import asyncio
from src.log import setup_logger

log = setup_logger(__name__)

async def upload_to_catbox(file_url: str, max_retries: int = 3) -> str | None:
    """
    Downloads an image from a URL and uploads it to Catbox.moe.
    Retries up to `max_retries` times on failure or timeout.
    Returns the Catbox URL if successful, otherwise None.
    """
    for attempt in range(1, max_retries + 1):
        try:
            async with aiohttp.ClientSession() as session:
                # Download the image
                async with session.get(file_url, timeout=15) as resp:
                    if resp.status != 200:
                        log.error(f"Failed to download image from {file_url}: Status {resp.status} (Attempt {attempt}/{max_retries})")
                        if attempt == max_retries: return None
                        await asyncio.sleep(2)
                        continue
                    file_data = await resp.read()

                # Upload to Catbox
                data = aiohttp.FormData()
                data.add_field('reqtype', 'fileupload')
                data.add_field('userhash', '') # Optional, leave empty for anonymous
                
                # Extract filename from URL or use a default
                filename = file_url.split('/')[-1].split('?')[0] or 'image.jpg'
                
                data.add_field('fileToUpload', file_data, filename=filename)

                async with session.post('https://catbox.moe/user/api.php', data=data, timeout=30) as resp:
                    if resp.status == 200:
                        catbox_url = await resp.text()
                        return catbox_url.strip()
                    else:
                        log.error(f"Failed to upload to Catbox: Status {resp.status} (Attempt {attempt}/{max_retries})")
                        if attempt == max_retries: return None
                        await asyncio.sleep(2)
                        continue

        except asyncio.TimeoutError:
            log.warning(f"Connection timeout to Catbox host (Attempt {attempt}/{max_retries}). Retrying...")
            if attempt < max_retries:
                await asyncio.sleep(3)
        except Exception as e:
            log.error(f"Error mirroring image to Catbox: {e} (Attempt {attempt}/{max_retries})")
            if attempt < max_retries:
                await asyncio.sleep(3)
            
    return None
