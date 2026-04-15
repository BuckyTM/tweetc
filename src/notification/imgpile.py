import aiohttp
import asyncio
import os
import io
from src.log import setup_logger

log = setup_logger(__name__)

async def download_for_discord(url: str, max_size: int = 25 * 1024 * 1024) -> io.BytesIO | None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30) as resp:
                if resp.status != 200:
                    return None
                length = resp.headers.get('Content-Length')
                if length and int(length) > max_size:
                    return None
                data = await resp.read()
                if len(data) > max_size:
                    return None
                return io.BytesIO(data)
    except Exception as e:
        log.error(f"Error downloading media for Discord attachment: {e}")
        return None

async def upload_to_imgpile(file_url: str, max_retries: int = 3) -> str | None:
    """
    Downloads an image from a URL and uploads it to Imgpile.com.
    Retries up to `max_retries` times on failure or timeout.
    Returns the Imgpile URL if successful, otherwise None.
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
                    
                    # Imgpile has a strict 100MB limit per their API docs
                    max_size_bytes = 100 * 1024 * 1024
                    content_length = resp.headers.get('Content-Length')
                    if content_length and int(content_length) > max_size_bytes:
                        log.warning(f"File too large to mirror ({int(content_length) / 1024 / 1024:.2f} MB): {file_url}")
                        return None

                    file_data = await resp.read()

                # Upload to Imgpile
                imgpile_token = os.getenv('IMGPILE_TOKEN', '')
                if not imgpile_token:
                    log.error("IMGPILE_TOKEN is not set in .env")
                    return None

                data = aiohttp.FormData()
                
                import mimetypes
                
                # Extract filename from URL or use a default
                filename = file_url.split('/')[-1].split('?')[0] or 'image.jpg'
                content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
                
                data.add_field('file', file_data, filename=filename, content_type=content_type)

                headers = {
                    'Authorization': f'Bearer {imgpile_token}'
                }

                async with session.post('https://cdn.imgpile.com/api/v1/media', data=data, headers=headers, timeout=30) as resp:
                    if resp.status in [200, 201]:
                        result = await resp.json()
                        media_obj = result.get('media', {})
                        
                        # Try to get from urls object first
                        imgpile_url = media_obj.get('urls', {}).get('original')
                        
                        # If urls object is missing, manually construct it per their docs
                        if not imgpile_url:
                            filename = media_obj.get('filename')
                            ext = media_obj.get('ext')
                            if filename and ext:
                                imgpile_url = f"https://cdn.imgpile.com/f/{filename}.{ext}"

                        if imgpile_url:
                            return imgpile_url
                            
                        log.error(f"Imgpile returned success but no original URL was found: {result}")
                        return None
                    else:
                        log.error(f"Failed to upload to Imgpile: Status {resp.status} (Attempt {attempt}/{max_retries})")
                        if attempt == max_retries: return None
                        await asyncio.sleep(2)
                        continue

        except asyncio.TimeoutError:
            log.warning(f"Connection timeout to Imgpile host (Attempt {attempt}/{max_retries}). Retrying...")
            if attempt < max_retries:
                await asyncio.sleep(3)
        except Exception as e:
            log.error(f"Error mirroring image to Imgpile: {e} (Attempt {attempt}/{max_retries})")
            if attempt < max_retries:
                await asyncio.sleep(3)
            
    return None
