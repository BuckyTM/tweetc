import re

import aiohttp
import discord
from bs4 import BeautifulSoup
from tweety.types import Tweet

from configs.load_configs import configs


async def gen_embed(tweet: Tweet) -> list[discord.Embed]:
    author = tweet.author
    embed = discord.Embed(title=f'{author.name} {get_action(tweet, disable_quoted=True)} {get_tweet_type(tweet)}', description=tweet.text, url=tweet.url, color=0x1da0f2, timestamp=tweet.created_on)
    embed.set_author(name=f'{author.name} (@{author.username})', icon_url=author.profile_image_url_https, url=f'https://twitter.com/{author.username}')
    embed.set_thumbnail(url=re.sub(r'normal(?=\.jpg$)', '400x400', tweet.author.profile_image_url_https))
    embed.set_footer(text='Twitter' if configs['embed']['built_in']['legacy_logo'] else 'X', icon_url='attachment://footer.png')
    if len(tweet.media) == 1:
        embed.set_image(url=tweet.media[0].media_url_https)
        return [embed]
    elif len(tweet.media) > 1:
        if configs['embed']['built_in']['fx_image']:
            try:
                async with aiohttp.ClientSession() as session:
                    # Use fxtwitter for embeds. Handle twitter.com and x.com
                    fx_url = re.sub(r'(?:twitter|x)\.com', 'fxtwitter.com', tweet.url)
                    async with session.get(fx_url) as response:
                        if response.status == 200:
                            raw = await response.text()
                            soup = BeautifulSoup(raw, 'html.parser')
                            # Try og:image first, then twitter:image
                            meta_tag = soup.find('meta', property='og:image') or soup.find('meta', attrs={'name': 'twitter:image'})
                            
                            if meta_tag and meta_tag.get('content'):
                                 fximage_url = meta_tag['content']
                                 embed.set_image(url=fximage_url)
                                 return [embed]
            except Exception:
                pass
            
            # Fallback if fximage fails or response is not 200
            imgs_embed = [discord.Embed(url=tweet.url).set_image(url=media.media_url_https) for media in tweet.media]
            imgs_embed.insert(0, embed)
            return imgs_embed
        else:
            imgs_embed = [discord.Embed(url=tweet.url).set_image(url=media.media_url_https) for media in tweet.media]
            imgs_embed.insert(0, embed)
            return imgs_embed
    return [embed]


def get_action(tweet: Tweet, disable_quoted: bool = False) -> str:
    if tweet.is_retweet:
        return 'retweeted'
    elif tweet.is_quoted and not disable_quoted:
        return 'quoted'
    else:
        return 'tweeted'


def get_tweet_type(tweet: Tweet) -> str:
    media = tweet.media
    if len(media) > 1:
        return f'{len(media)} photos'
    elif len(media) == 1:
        return f'a {media[0].type}'
    else:
        return 'a status'
