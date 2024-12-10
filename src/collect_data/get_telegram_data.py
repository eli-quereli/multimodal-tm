import logging
import asyncio
from datetime import date, datetime
from random import randrange
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto, MessageMediaWebPage
from telethon.tl.functions.channels import GetFullChannelRequest
# from telethon.tl.types import InputPeerChannel
from telethon import errors


def create_media_dirs(channel_name, DATA_DIR):
    """Create directories to store media for a specific channel."""
    img_dir = DATA_DIR / channel_name / 'images'
    audio_dir = DATA_DIR / channel_name / 'audio'
    img_dir.mkdir(parents=True, exist_ok=True)
    audio_dir.mkdir(parents=True, exist_ok=True)
    return img_dir, audio_dir


async def download_media(client, msg_type, message, img_dir, audio_dir):
    """Download media files from messages."""
    if msg_type == 'image':
        media = message.media.photo
        dir_path = img_dir
    elif msg_type == 'audio':
        media = message.media.document
        dir_path = audio_dir
    else:
        return None, None
    access_hash = str(media.access_hash)
    media_id = str(media.id)
    file_path = f"{dir_path}/{media_id}"
    try:
        await client.download_media(message=message, file=file_path)
    except Exception:
        logging.exception(f"Exception during {msg_type} download.")
    
    return str(access_hash), str(media_id)


def setup_logging(log_file_prefix: str = "get_telegram_data"):
    """Configure logging for the script."""
    log_file = f"../logs/{log_file_prefix}_{datetime.now().strftime('%Y_%m_%d')}.log"
    logging.basicConfig(
        filename=log_file,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )


def create_post(channel_name, message):
    """Create dictionary for a single message."""

    post = {
        'channel_name': channel_name,
        'channel_id': str(message.peer_id.channel_id),
        'message_id': str(message.id),
        'custom_id': f"{channel_name}_{message.id}",
        'via_bot_id': message.via_bot_id,
        'msg_date': message.date,
        'edit_date': message.edit_date,
        'text': message.text,
        'views': str(message.views),
        'forwards': str(message.forwards),
        'fwd_from': str(message.fwd_from),
        'replies': str(message.replies),
        'reply_to': str(message.reply_to),
        'media': message.media,
        'media_id': None,
        'access_hash': None,
        'collected_on': date.today().strftime('%Y-%m-%d')
    }
    return post


def get_message_type(message):
    if not message.text and not message.media:
        return 'service'
    
    elif not message.media and message.text:
        return 'text'

    elif message.media:
        #  images
        if isinstance(message.media, MessageMediaPhoto):
            return 'image'
        elif isinstance(message.media, MessageMediaDocument):
        # audio
            if 'audio' in message.media.document.mime_type:  
                return 'audio'
        #  video
            elif 'video' in message.media.document.mime_type:
                return 'video'
        #  web page
        elif isinstance(message.media, MessageMediaWebPage):
            return 'web_page'
        # other media
        else: 
            return 'other'


async def get_messages(client, channel_name, channel_id, access_hash, start_date, end_date, data_dir):
    """Fetch messages from a specific channel."""
    # try: 
    # chat = InputPeerChannel(channel_id, access_hash)
    chat = await client.get_input_entity(channel_id)
    channel_info = await client(GetFullChannelRequest(chat))
    participant_count = await client.get_participants(chat, limit=0)
    img_dir, audio_dir = create_media_dirs(channel_name, data_dir)
    print(f"Fetching messages for channel: {channel_name}")
    all_messages = []
    async for message in client.iter_messages(chat, offset_date=end_date):
        if message.date < start_date:  # stop when we reach the start date
            break
        if start_date <= message.date <= end_date:
            # print(message.date)
            post = create_post(channel_name, message)
            msg_type = get_message_type(message)
            post['msg_type'] = msg_type
            post['participant_count'] = participant_count.total
            post['channel_description'] = channel_info.full_chat.about

            if post['msg_type'] in ['audio', 'image']:
                try:
                    access_hash, media_id = await download_media(
                        client,
                        msg_type,
                        message,
                        img_dir,
                        audio_dir
                        )
                    post['access_hash'] = str(access_hash)
                    post['media_id'] = str(media_id)
                    await asyncio.sleep(1)
                except Exception:
                    logging.exception(
                            """Exception occurred during media download
                            for channel: %s, media_id: %s, access_hash: %s""",
                            channel_name, media_id, access_hash
                        )
        all_messages.append(post)
    time_to_wait = 10.0 + randrange(20)/10
    print(f"Sleeping {time_to_wait} seconds.")
    await asyncio.sleep(time_to_wait)
    return all_messages