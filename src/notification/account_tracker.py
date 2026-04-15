import asyncio
import os
import sys
import re
from datetime import datetime, timezone, timedelta

import aiosqlite
import discord
from discord.ext import commands
from tweety import Twitter

from configs.load_configs import configs
from src.log import setup_logger
from src.notification.imgpile import upload_to_imgpile, download_for_discord
from src.notification.display_tools import gen_embed, get_action
from src.notification.get_tweets import get_tweets
from src.notification.tweet_media import fetch_tweet_media
from src.notification.utils import is_match_media_type, is_match_type, replace_emoji
from src.utils import get_accounts, get_lock, get_utcnow
from src.db_function.readonly_db import connect_readonly
from src.db_function.init_db import init_latest_tweet_on_startup

EMBED_TYPE: str = configs['embed']['type']
SERVICE: str = configs['embed']['proxy']['service']
DOMAIN_NAME: str = configs['embed']['proxy']['domain_name']
AUTO_TRANSLATION: dict[bool, str] = configs['embed']['proxy']['auto_translation']

log = setup_logger(__name__)
lock = get_lock()

class AccountTracker():
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.accounts_data = get_accounts()
        self.db_path = os.path.join(os.getenv('DATA_PATH'), 'tracked_accounts.db')
        self.tweets = {account_name: [] for account_name in self.accounts_data.keys()}
        # Responsible for processing queries and writing timestamps
        self.db_write_queue = asyncio.Queue()
        self.latest_tweet_timestamps = {}
        self.timestamps_ready = asyncio.Event()

        self.tasksMonitorLogAt = datetime.now(timezone.utc) - timedelta(hours=configs['tasks_monitor_log_period'])
        bot.loop.create_task(self.setup_tasks())

    async def setup_tasks(self):
        if configs['init_latest_tweet_on_startup']:
            await init_latest_tweet_on_startup(self.db_path)

        # Start the core database workers first
        self.bot.loop.create_task(self.timestamp_updater()).set_name('TimestampUpdater')
        self.bot.loop.create_task(self.db_writer()).set_name('DBWriter')

        # Wait for the initial timestamp load
        await self.timestamps_ready.wait()

        async def authenticate_account(account_name, account_token):
            app = Twitter(account_name)
            max_attempts = configs['auth_max_attempts']
            for attempt in range(max_attempts):
                try:
                    await app.load_auth_token(account_token)
                    return app
                except Exception as e:
                    log.error(f"authentication failed for account: {account_name} [Attempt {attempt + 1}/{max_attempts}]")
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(5)
                    else:
                        log.error(f"persistent authentication failure for account {account_name}")
                        raise
        
        for account_name, account_token in self.accounts_data.items():
            try:
                app = await authenticate_account(account_name, account_token)
                self.bot.loop.create_task(self.tweetsUpdater(app)).set_name(f'TweetsUpdater_{account_name}')
            except Exception:
                sys.exit(1)

        # Initial user list for notification tasks
        for (username, client_used), _ in self.latest_tweet_timestamps.items():
            self.bot.loop.create_task(self.notification(username, client_used, skip_first_cycle=True)).set_name(username)
        
        self.bot.loop.create_task(self.tasksMonitor()).set_name('TasksMonitor')

    async def timestamp_updater(self):
        """Periodically reads all user timestamps from the DB into a shared dictionary."""
        while True:
            try:
                async with connect_readonly(self.db_path) as db:
                    async with db.execute('SELECT username, client_used, latest_tweet FROM user WHERE enabled = 1') as cursor:
                        db_timestamps = {}
                        async for row in cursor:
                            db_timestamps[(row[0], row[1])] = row[2]

                if not self.timestamps_ready.is_set():
                    # First load: set the entire dictionary
                    self.latest_tweet_timestamps = db_timestamps
                    self.timestamps_ready.set()
                    log.info("initial tweet timestamps loaded")
                else:
                    # Subsequent loads: merge carefully to avoid overwriting
                    # fresher in-memory values with stale DB values.
                    # 1) Add any new users from DB that we don't have yet
                    for key, db_ts in db_timestamps.items():
                        if key not in self.latest_tweet_timestamps:
                            self.latest_tweet_timestamps[key] = db_ts
                    # 2) Remove users no longer in the DB (disabled/deleted)
                    for key in list(self.latest_tweet_timestamps.keys()):
                        if key not in db_timestamps:
                            del self.latest_tweet_timestamps[key]

            except Exception as e:
                log.error(f"error in timestamp_updater: {e}")

            # After careful consideration, it was decided to keep it hard-coded, as it makes little sense to allow users to customize this value.
            await asyncio.sleep(60)

    async def db_writer(self):
        """Singleton task to handle all database write operations."""
        while True:
            try:
                username, new_timestamp = await self.db_write_queue.get()
                async with lock:
                    async with aiosqlite.connect(self.db_path, timeout=10) as db:
                        await db.execute('UPDATE user SET latest_tweet = ? WHERE username = ?', (str(new_timestamp), username))
                        await db.commit()
                self.db_write_queue.task_done()
            except Exception as e:
                log.error(f"error in db_writer: {e}")

    async def notification(self, username: str, client_used: str, skip_first_cycle: bool = False):
        first_cycle = skip_first_cycle
        while True:
            await asyncio.sleep(configs['tweets_check_period'])

            last_tweet_at = self.latest_tweet_timestamps.get((username, client_used))
            if not last_tweet_at:
                # This can happen if a user is removed right after the sleep.
                log.warning(f"no timestamp for {username}, task will terminate.")
                break

            latest_tweets = await get_tweets(self.tweets[client_used], username, last_tweet_at)
            if not latest_tweets:
                first_cycle = False
                continue
            
            newest_timestamp = latest_tweets[-1].created_on
            # Update local cache immediately to prevent re-notification
            self.latest_tweet_timestamps[(username, client_used)] = str(newest_timestamp)
            # Queue the database update
            await self.db_write_queue.put((username, newest_timestamp))

            if first_cycle:
                first_cycle = False
                log.info(f"skipped {len(latest_tweets)} tweet(s) for {username} on first cycle after startup to prevent re-notification")
                continue

            user = None
            notifications = []
            try:
                async with connect_readonly(self.db_path) as db:
                    db.row_factory = aiosqlite.Row
                    async with db.cursor() as cursor:
                        await cursor.execute('SELECT id FROM user WHERE username = ?', (username,))
                        user = await cursor.fetchone()
                        if user:
                            if EMBED_TYPE == 'proxy' and AUTO_TRANSLATION['enabled']:
                                await cursor.execute('''
                                    SELECT n.*, suc.translate AS server_translate
                                    FROM notification n
                                    JOIN channel c ON n.channel_id = c.id
                                    LEFT JOIN server_user_config suc ON c.server_id = suc.server_id AND n.user_id = suc.user_id
                                    WHERE n.user_id = ? AND n.enabled = 1
                                ''', (user['id'],))
                            else:
                                await cursor.execute('SELECT * FROM notification WHERE user_id = ? AND enabled = 1', (user['id'],))
                            notifications = await cursor.fetchall()
            except aiosqlite.OperationalError as e:
                if "database is locked" in str(e):
                    log.warning(f"database locked while reading notification settings for {username}, this is unexpected but handled.")
                else:
                    raise
            
            if not user:
                continue

            for tweet in latest_tweets:
                log.info(f'find a new tweet from {username}')
                mirror_embeds = []
                discord_files = []
                failed_mirrors = 0
                
                if configs.get('mirror_media', {}).get('enabled', False) and tweet.media:
                    tweet_data = await fetch_tweet_media(tweet.url)
                    if tweet_data and tweet_data.get('media'):
                        for media in tweet_data['media']:
                            if media['type'] == 'photo' or media['type'] == 'gif':
                                imgpile_url = await upload_to_imgpile(media['url'])
                                if imgpile_url:
                                    if media['type'] == 'photo':
                                        embed = discord.Embed(title="Mirrored Image", url=imgpile_url, description=f"[Link to image]({imgpile_url})")
                                        embed.set_image(url=imgpile_url)
                                        mirror_embeds.append(embed)
                                    # GIFs just need a link to embed, but since we are mirroring we might as well attach them if they are small enough, 
                                    # but wait, user asked GIFs to Imgpile. For auto-notifications, if we just send the imgpile url, Discord will embed it.
                                    # However, account_tracker already sends embeds array. To embed a GIF from imgpile natively, it's better to just add the URL to the main message content or as an embed.
                                    elif media['type'] == 'gif':
                                        embed = discord.Embed(title="Mirrored GIF", url=imgpile_url)
                                        embed.set_image(url=imgpile_url)
                                        mirror_embeds.append(embed)
                                else:
                                    failed_mirrors += 1
                            else:
                                # Video
                                video_bytes = await download_for_discord(media['url'])
                                if video_bytes:
                                    filename = f"video_{tweet.id}.mp4"
                                    # Store raw bytes and filename to recreate discord.File for multiple channels
                                    discord_files.append((video_bytes.getvalue(), filename))
                                else:
                                    # We don't append raw URL here because the built_in embed handles it via the "View Video" button below
                                    # but we can increment failed_mirrors to warn them the attachment failed.
                                    pass

                url = tweet.url
                url = re.sub(r'(?:twitter|x)\.com', f'{DOMAIN_NAME}.com', url)
                if EMBED_TYPE == 'proxy' and AUTO_TRANSLATION['enabled']:
                    url += f"/{AUTO_TRANSLATION['default_language']}"
                
                view, create_view = None, False
                if bool(tweet.media) and tweet.media[0].type == 'video' and EMBED_TYPE == 'built_in' and configs['embed']['built_in']['video_link_button']:
                    create_view = True
                    button_label, button_url = 'View Video', tweet.media[0].expanded_url
                elif EMBED_TYPE == 'proxy' and configs['embed']['proxy']['original_url_button']:
                    create_view = True
                    button_label, button_url = 'View Original', tweet.url

                if create_view:
                    view = discord.ui.View()
                    view.add_item(discord.ui.Button(label=button_label, style=discord.ButtonStyle.link, url=button_url))
                
                for data in notifications:
                    channel_id = int(data['channel_id'])
                    channel = self.bot.get_channel(channel_id)
                    if channel is None:
                        # Threads (e.g. forum posts) aren't returned by get_channel
                        for guild in self.bot.guilds:
                            channel = guild.get_thread(channel_id)
                            if channel is not None:
                                break
                    if channel is not None and is_match_type(tweet, data['enable_type']) and is_match_media_type(tweet, data['enable_media_type']):
                        try:
                            url = tweet.url
                            if EMBED_TYPE == 'proxy':
                                url = url.replace('twitter', DOMAIN_NAME)
                                if AUTO_TRANSLATION['enabled']:
                                    lang = data['server_translate'] if data['server_translate'] is not None else AUTO_TRANSLATION['default_language']
                                    url += f"/{lang}"

                            mention = f"{channel.guild.get_role(int(data['role_id'])).mention} " if data['role_id'] else ''
                            author, action = tweet.author.name, get_action(tweet)
                            
                            if not data['customized_msg']: msg = configs['default_message']
                            else: msg = re.sub(r":(\w+):", lambda match: replace_emoji(match, channel.guild), data['customized_msg']) if configs['emoji_auto_format'] else data['customized_msg']
                            msg = msg.format(mention=mention, author=author, action=action, url=url)
                            
                            if failed_mirrors > 0:
                                msg += f"\n\n*⚠️ Warning: Failed to mirror {failed_mirrors} image(s) to Imgpile (Filehost timeout or file too large).* "

                            if EMBED_TYPE == 'proxy':
                                import io
                                to_send = [discord.File(fp=io.BytesIO(b), filename=f) for b, f in discord_files]
                                await channel.send(msg, view=view, embeds=mirror_embeds if mirror_embeds else [], files=to_send)
                            else:
                                import io
                                footer = 'twitter.png' if configs['embed']['built_in']['legacy_logo'] else 'x.png'
                                to_send = [discord.File(f'images/{footer}', filename='footer.png')]
                                to_send.extend([discord.File(fp=io.BytesIO(b), filename=f) for b, f in discord_files])
                                
                                embeds = await gen_embed(tweet)
                                if mirror_embeds:
                                    embeds.extend(mirror_embeds)
                                await channel.send(msg, files=to_send, embeds=embeds, view=view)

                        except Exception as e:
                            if not isinstance(e, discord.errors.Forbidden):
                                log.error(f'an error occurred at {channel.mention} while sending notification: {e}')

    async def tweetsUpdater(self, app: Twitter):
        updater_name = asyncio.current_task().get_name().split('_', 1)[1]
        while True:
            try:
                # Run the potentially blocking library call in a separate thread
                self.tweets[updater_name] = await asyncio.to_thread(app.get_tweet_notifications)
            except KeyError as e:
                # Handle the error thrown by `tweety-ns` mentioned in issue#59
                log.warning(f"handled KeyError in {updater_name}: {e}. This is likely a temporary API response issue from Twitter. Skipping this check.")
            except Exception as e:
                log.error(f'{e} (task : tweets updater {updater_name})')
                log.error(f"an unexpected error occurred, try again in {configs['tweets_updater_retry_delay']} minutes")
                await asyncio.sleep(configs['tweets_updater_retry_delay'] * 60)
                continue
            
            await asyncio.sleep(configs['tweets_check_period'])

    async def tasksMonitor(self):
        """Dynamically monitors tasks based on the live timestamp cache."""
        while True:
            await asyncio.sleep(configs['tasks_monitor_check_period'] * 60)

            running_tasks = {task.get_name() for task in asyncio.all_tasks()}
            users_in_cache = {username for username, _ in self.latest_tweet_timestamps.keys()}
            
            alive_tasks = running_tasks & users_in_cache

            if alive_tasks != users_in_cache:
                dead_tasks = list(users_in_cache - alive_tasks)
                if dead_tasks:
                    log.warning(f'dead tasks : {dead_tasks}')
                    for dead_task_username in dead_tasks:
                        # Find the corresponding client_used from the cache
                        client_used = None
                        for u, c in self.latest_tweet_timestamps.keys():
                            if u == dead_task_username:
                                client_used = c
                                break
                        
                        if client_used:
                            self.bot.loop.create_task(self.notification(dead_task_username, client_used)).set_name(dead_task_username)
                            log.info(f'restart {dead_task_username} successfully using {client_used}')

            for client in self.accounts_data.keys():
                if f'TweetsUpdater_{client}' not in running_tasks:
                    log.warning(f'tweets updater {client} : dead')

            if (datetime.now(timezone.utc) - self.tasksMonitorLogAt).total_seconds() / 3600 >= configs['tasks_monitor_log_period']:
                log.info(f'alive tasks : {list(alive_tasks)}')
                for client in self.accounts_data.keys():
                    if f'TweetsUpdater_{client}' in running_tasks:
                        log.info(f'tweets updater {client} : alive')
                self.tasksMonitorLogAt = datetime.now(timezone.utc)


    async def addTask(self, username: str, client_used: str):
        """Adds a new user to the live cache and starts their notification task."""
        # Add to live cache first
        self.latest_tweet_timestamps[(username, client_used)] = get_utcnow()
        
        # Start the task
        self.bot.loop.create_task(self.notification(username, client_used)).set_name(username)
        log.info(f'new task {username} added successfully using {client_used}')

    async def removeTask(self, username: str):
        """Removes a user from the live cache and cancels their notification task."""
        key_to_remove = None
        # Create a copy of keys for safe iteration
        for u, c in list(self.latest_tweet_timestamps.keys()):
            if u == username:
                key_to_remove = (u, c)
                break
        
        # Remove from cache so the monitor doesn't restart it
        if key_to_remove and key_to_remove in self.latest_tweet_timestamps:
            del self.latest_tweet_timestamps[key_to_remove]

        # Cancel the running task
        for task in asyncio.all_tasks():
            if task.get_name() == username:
                task.cancel()
                log.info(f'task {username} has been cancelled')
                break
