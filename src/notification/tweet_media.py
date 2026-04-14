import re
import aiohttp
from src.log import setup_logger

log = setup_logger(__name__)

TWEET_URL_PATTERN = re.compile(
    r'https?://(?:twitter|x|fxtwitter|vxtwitter|fixupx)\.com/(\w+)/status/(\d+)'
)


async def fetch_tweet_media(tweet_url: str) -> dict | None:
    """
    Fetches media information for a tweet using the fxtwitter API.
    Returns a dict with 'media' list and 'author' name, or None on failure.
    Each media item has 'type' ('photo'/'video'/'gif'), 'url', and optionally 'thumbnail_url'.
    """
    match = TWEET_URL_PATTERN.search(tweet_url)
    if not match:
        return None

    username, tweet_id = match.group(1), match.group(2)
    api_url = f'https://api.fxtwitter.com/{username}/status/{tweet_id}'

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as resp:
                if resp.status != 200:
                    log.error(f"fxtwitter API returned status {resp.status} for {api_url}")
                    return None
                data = await resp.json()

        tweet = data.get('tweet', {})
        media_section = tweet.get('media', {})
        all_media = media_section.get('all', [])

        if not all_media:
            return None

        result = []
        for item in all_media:
            media_type = item.get('type')  # 'photo', 'video', 'gif'
            media_url = item.get('url')

            if not media_type or not media_url:
                continue

            entry = {
                'type': media_type,
                'url': media_url,
                'thumbnail_url': item.get('thumbnail_url'),
            }

            # For videos/GIFs, prefer 720p mp4 to keep file sizes reasonable for catbox
            if media_type in ('video', 'gif'):
                formats = item.get('formats', [])
                mp4_formats = [f for f in formats if f.get('container') == 'mp4']
                if mp4_formats:
                    mp4_formats.sort(key=lambda f: f.get('bitrate', 0))
                    picked = mp4_formats[-1]  # default to highest quality
                    for fmt in reversed(mp4_formats):
                        if fmt.get('bitrate', 0) <= 2500000:  # ~720p
                            picked = fmt
                            break
                    entry['url'] = picked['url']

            result.append(entry)

        return {'media': result, 'author': tweet.get('author', {}).get('name', 'Unknown')}

    except Exception as e:
        log.error(f"Error fetching tweet media from fxtwitter: {e}")
        return None
