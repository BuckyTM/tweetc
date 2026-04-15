import os
import io
import aiohttp
from typing import Union

import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands

from core.classes import Cog_Extension
from src.log import setup_logger
from src.notification.imgpile import upload_to_imgpile
from src.notification.tweet_media import fetch_tweet_media, TWEET_URL_PATTERN
from src.permission import ADMINISTRATOR
from src.utils import get_lock

log = setup_logger(__name__)
lock = get_lock()

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

MEDIA_TYPE_CHOICES = [
    app_commands.Choice(name='All (images, videos, GIFs)', value='images,videos,gifs'),
    app_commands.Choice(name='Images only', value='images'),
    app_commands.Choice(name='Videos only', value='videos'),
    app_commands.Choice(name='GIFs only', value='gifs'),
    app_commands.Choice(name='Images + Videos', value='images,videos'),
    app_commands.Choice(name='Images + GIFs', value='images,gifs'),
    app_commands.Choice(name='Videos + GIFs', value='videos,gifs'),
]

# Map fxtwitter media types to our config names
MEDIA_TYPE_MAP = {
    'photo': 'images',
    'video': 'videos',
    'gif': 'gifs',
}


class Mirror(Cog_Extension):
    def __init__(self, bot):
        super().__init__(bot)
        self.db_path = os.path.join(os.getenv('DATA_PATH'), 'tracked_accounts.db')

    mirror_group = app_commands.Group(
        name='mirror',
        description='Configure automatic media mirroring for Twitter/X links',
        default_permissions=ADMINISTRATOR
    )

    def _resolve_channel(self, channel, itn: discord.Interaction):
        """Resolve the target channel. If no channel is given and the command is
        invoked from inside a forum thread, resolve to the parent forum channel."""
        if channel is not None:
            return channel
        ch = itn.channel
        if isinstance(ch, discord.Thread) and isinstance(ch.parent, discord.ForumChannel):
            return ch.parent
        return ch

    @mirror_group.command(name='enable')
    async def mirror_enable(self, itn: discord.Interaction, channel: Union[discord.TextChannel, discord.ForumChannel] = None):
        """Enable media mirroring for a channel.

        Parameters
        -----------
        channel: Union[discord.TextChannel, discord.ForumChannel]
            The channel to enable mirroring in. Defaults to the current channel (or its parent forum).
        """
        channel = self._resolve_channel(channel, itn)
        await itn.response.defer(ephemeral=True)

        async with lock:
            async with aiosqlite.connect(self.db_path, timeout=10) as db:
                await db.execute(
                    'INSERT INTO mirror_channel (channel_id, server_id, enabled, media_types) '
                    'VALUES (?, ?, 1, ?) '
                    'ON CONFLICT(channel_id) DO UPDATE SET enabled = 1',
                    (str(channel.id), str(channel.guild.id), 'images,videos,gifs')
                )
                await db.commit()

        await itn.followup.send(
            f'Media mirroring enabled in {channel.mention}. '
            f'When users post Twitter/X links, the bot will mirror media to catbox.moe.',
            ephemeral=True
        )

    @mirror_group.command(name='disable')
    async def mirror_disable(self, itn: discord.Interaction, channel: Union[discord.TextChannel, discord.ForumChannel] = None):
        """Disable media mirroring for a channel.

        Parameters
        -----------
        channel: Union[discord.TextChannel, discord.ForumChannel]
            The channel to disable mirroring in. Defaults to the current channel (or its parent forum).
        """
        channel = self._resolve_channel(channel, itn)
        await itn.response.defer(ephemeral=True)

        async with lock:
            async with aiosqlite.connect(self.db_path, timeout=10) as db:
                await db.execute(
                    'UPDATE mirror_channel SET enabled = 0 WHERE channel_id = ?',
                    (str(channel.id),)
                )
                await db.commit()

        await itn.followup.send(f'Media mirroring disabled in {channel.mention}.', ephemeral=True)

    @mirror_group.command(name='media_types')
    @app_commands.choices(types=MEDIA_TYPE_CHOICES)
    async def mirror_media_types(self, itn: discord.Interaction, types: str, channel: Union[discord.TextChannel, discord.ForumChannel] = None):
        """Configure which media types to mirror.

        Parameters
        -----------
        types: str
            The media types to mirror.
        channel: Union[discord.TextChannel, discord.ForumChannel]
            The channel to configure. Defaults to the current channel (or its parent forum).
        """
        channel = self._resolve_channel(channel, itn)
        await itn.response.defer(ephemeral=True)

        async with lock:
            async with aiosqlite.connect(self.db_path, timeout=10) as db:
                await db.execute(
                    'INSERT INTO mirror_channel (channel_id, server_id, enabled, media_types) '
                    'VALUES (?, ?, 1, ?) '
                    'ON CONFLICT(channel_id) DO UPDATE SET media_types = excluded.media_types',
                    (str(channel.id), str(channel.guild.id), types)
                )
                await db.commit()

        await itn.followup.send(
            f'Media types for {channel.mention} set to: **{types}**',
            ephemeral=True
        )

    @mirror_group.command(name='status')
    async def mirror_status(self, itn: discord.Interaction, channel: Union[discord.TextChannel, discord.ForumChannel] = None):
        """Show the current mirror configuration for a channel.

        Parameters
        -----------
        channel: Union[discord.TextChannel, discord.ForumChannel]
            The channel to check. Defaults to the current channel (or its parent forum).
        """
        channel = self._resolve_channel(channel, itn)
        await itn.response.defer(ephemeral=True)

        async with aiosqlite.connect(self.db_path, timeout=10) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                'SELECT * FROM mirror_channel WHERE channel_id = ?', (str(channel.id),)
            ) as cursor:
                row = await cursor.fetchone()

        if row:
            status = 'Enabled' if row['enabled'] else 'Disabled'
            media_types = row['media_types']
            await itn.followup.send(
                f'**Mirror status for {channel.mention}:**\n'
                f'Status: {status}\n'
                f'Media types: {media_types}',
                ephemeral=True
            )
        else:
            await itn.followup.send(
                f'Media mirroring is not configured for {channel.mention}. '
                f'Use `/mirror enable` to set it up.',
                ephemeral=True
            )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Ignore bot messages
        if message.author.bot:
            return

        # Check if the message contains a Twitter/X link
        matches = TWEET_URL_PATTERN.findall(message.content)
        if not matches:
            return

        # Check if mirroring is enabled for this channel
        # For forum threads, also check the parent forum channel
        channel_ids_to_check = [str(message.channel.id)]
        if isinstance(message.channel, discord.Thread) and message.channel.parent_id:
            channel_ids_to_check.append(str(message.channel.parent_id))

        try:
            async with aiosqlite.connect(self.db_path, timeout=10) as db:
                db.row_factory = aiosqlite.Row
                placeholders = ','.join('?' for _ in channel_ids_to_check)
                async with db.execute(
                    f'SELECT * FROM mirror_channel WHERE channel_id IN ({placeholders}) AND enabled = 1',
                    channel_ids_to_check
                ) as cursor:
                    config = await cursor.fetchone()
        except Exception as e:
            log.error(f"Error checking mirror config: {e}")
            return

        if not config:
            return

        allowed_types = set(config['media_types'].split(','))

        # Process each Twitter link found in the message
        for username, tweet_id in matches:
            tweet_url = f'https://twitter.com/{username}/status/{tweet_id}'
            tweet_data = await fetch_tweet_media(tweet_url)

            if not tweet_data or not tweet_data.get('media'):
                continue

            embeds = []
            video_urls = []
            discord_files = []
            failed_mirrors = 0

            for media in tweet_data['media']:
                media_config_name = MEDIA_TYPE_MAP.get(media['type'])
                if not media_config_name or media_config_name not in allowed_types:
                    continue

                if media['type'] == 'photo':
                    imgpile_url = await upload_to_imgpile(media['url'])
                    if not imgpile_url:
                        failed_mirrors += 1
                        continue
                    embed = discord.Embed()
                    embed.set_image(url=imgpile_url)
                    embeds.append(embed)
                elif media['type'] == 'gif':
                    # Imgpile handles GIFs natively, so upload to Imgpile
                    imgpile_url = await upload_to_imgpile(media['url'])
                    if not imgpile_url:
                        failed_mirrors += 1
                        continue
                    video_urls.append(imgpile_url)
                else:
                    # Videos: Download to memory and attach directly to Discord message (25MB limit)
                    # This prevents the video from being lost if the tweet is deleted.
                    video_bytes = await download_for_discord(media['url'])
                    if video_bytes:
                        # Create discord.File object
                        filename = f"video_{tweet_id}.mp4"
                        discord_files.append(discord.File(fp=video_bytes, filename=filename))
                    else:
                        # If video is >25MB or download fails, fallback to raw Twitter URL
                        video_urls.append(media['url'])

            if not embeds and not video_urls and not discord_files and failed_mirrors == 0:
                continue

            content_parts = []
            if video_urls:
                content_parts.extend(video_urls)
            if failed_mirrors > 0:
                content_parts.append(f"\n*⚠️ Warning: Failed to mirror {failed_mirrors} media file(s) to Imgpile (Filehost timeout or file too large).*")
            
            content = '\n'.join(content_parts) if content_parts else None

            try:
                await message.reply(
                    content=content,
                    embeds=embeds if embeds else [],
                    files=discord_files,
                    mention_author=False
                )
            except Exception as e:
                log.error(f"Error sending mirror reply: {e}")


async def setup(bot: commands.Bot):
    # Ensure the mirror_channel table exists for existing databases
    db_path = os.path.join(os.getenv('DATA_PATH'), 'tracked_accounts.db')
    async with aiosqlite.connect(db_path, timeout=10) as db:
        await db.execute(
            'CREATE TABLE IF NOT EXISTS mirror_channel '
            '(channel_id TEXT PRIMARY KEY, server_id TEXT, enabled INTEGER DEFAULT 1, '
            "media_types TEXT DEFAULT 'images,videos,gifs')"
        )
        await db.commit()

    await bot.add_cog(Mirror(bot))
