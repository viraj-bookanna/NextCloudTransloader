import os,aiohttp,asyncio,urllib.parse,time,hashlib,logging
from telethon import TelegramClient,events,Button
from telethon.sessions import StringSession
from telethon.tl.types import MessageEntityUrl
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from http import HTTPStatus

load_dotenv(override=True)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
DOWNLOAD_TIMEOUT_MINUTES = int(os.getenv('DOWNLOAD_TIMEOUT_MINUTES', '10'))

def humanify(byte_size):
    siz_list = ['KB', 'MB', 'GB']
    for i in range(len(siz_list)):
        if byte_size/1024**(i+1) < 1024:
            return "{} {}".format(round(byte_size/1024**(i+1), 2), siz_list[i])
def progress_bar(percentage, progressbar_length=10):
    prefix_char = '█'
    suffix_char = '▒'
    fill = round(progressbar_length*percentage/100)
    prefix = fill * prefix_char
    suffix = (progressbar_length-fill) * suffix_char
    return f"{prefix}{suffix} {percentage:.2f}%"
class TimeKeeper:
    def __init__(self, title=''):
        self.title = title
        self.last_percentage = 0
        self.last_edited_time = 0
async def prog_callback(desc, current, total, event, file_name, tk):
    percentage = current/total*100
    if tk.last_percentage+2 < percentage and tk.last_edited_time+5 < time.time():
        await event.edit(f"{tk.title}\n**{desc}ing**: {progress_bar(percentage)}\n**File Name**: {file_name}\n**Size**: {humanify(total)}\n**{desc}ed**: {humanify(current)}")
        tk.last_percentage = percentage
        tk.last_edited_time = time.time()
def parse_header(header):
    header = header.split(';', 1)
    if len(header)==1:
        return header[0].strip(), {}
    params = [p.split('=') for p in header[1].split(';')]
    return header[0].strip(), {key[0].strip(): key[1].strip('" ') for key in params}
async def callback_pipe(source_stream, total, progress_callback):
    downloaded = 0
    async for chunk in source_stream:
        yield chunk
        downloaded += len(chunk)
        if progress_callback and total:
            await progress_callback(downloaded, total)
async def stream_download_to_nextcloud(download_url, user, message, title=''):
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),
        timeout=aiohttp.ClientTimeout(total=60*DOWNLOAD_TIMEOUT_MINUTES)
    ) as session:
        parsed_url = urllib.parse.urlparse(download_url)
        down_headers = {
            'Accept': '*/*',
            'Referer': f'{parsed_url.scheme}://{parsed_url.netloc}/',
        }
        async with session.get(download_url, headers=down_headers) as resp:
            file_org_name = urllib.parse.unquote(os.path.basename(urllib.parse.urlparse(str(resp.url)).path))
            server_filename = parse_header(resp.headers.get('content-disposition', ''))[1].get('filename', None)
            if resp.status != 200:
                raise Exception(f"Failed to download file: {resp.status}")
            total = int(resp.headers.get('content-length', 0)) or None
            if server_filename:
                file_org_name = server_filename
            if file_org_name == '' or len(file_org_name) > 250:
                file_org_name = hashlib.md5(download_url.encode()).hexdigest()
            up_headers = {
                "Content-Type": "application/octet-stream",
                "Content-Length": str(total)
            }
            tk = TimeKeeper(title)
            progress_callback = lambda c,t:prog_callback('Transload', c, t, message, file_org_name, tk)
            async with session.put(
                f"{user['nextcloud_domain']}/public.php/webdav/{file_org_name}",
                data=callback_pipe(resp.content.iter_chunked(1024), total, progress_callback),
                auth=aiohttp.BasicAuth(user['folder_key'], ""),
                headers=up_headers,
            ) as put_resp:
                if put_resp.status not in [200, 201, 204]:
                    raise Exception(f"Error-Code: {put_resp.status} ({HTTPStatus(put_resp.status).phrase})")
                delmsg = ''
                if user.get('immdel_on'):
                    await session.delete(
                        f"{user['nextcloud_domain']}/public.php/webdav/{file_org_name}",
                        auth=aiohttp.BasicAuth(user['folder_key'], ""),
                    )
                    delmsg = ' (Immediate Deletion)'
                await message.edit(f"**File Name**: {file_org_name}\n**Size**: {humanify(total)}\nTransfer Successful ✅{delmsg}", buttons=[[Button.url("Open Folder 🔗", f"{user['nextcloud_domain']}/s/{user['folder_key']}")]])
def find_all_urls(message):
    ret = list()
    if message.entities is None:
        return ret
    for entity in message.entities:
        if type(entity) == MessageEntityUrl:
            url = message.text[entity.offset:entity.offset+entity.length]
            if url.startswith('http://') or url.startswith('https://'):
                ret.append(url)
            else:
                ret.append('http://'+url)
    return ret

bot = TelegramClient('nextpipe', 6, 'eb06d4abfb49dc3eeb1aeb98ae0f581e').start(bot_token=os.environ['BOT_TOKEN'])
mongo_client = MongoClient(os.environ['MONGODB_URI'], server_api=ServerApi('1'))
collection = mongo_client.nextcloud_pipe.users
direct_reply = {
    '/start': "Hi",
    '/help': "You can create a public folder in your nextcloud account and get the share link\nThe link looks like: `https://cloud.example.lk/s/aBcDEfGH`\nYou have to send this link to /add_folder",
}

@bot.on(events.NewMessage(func=lambda e: e.is_private))
async def handler(event):
    user = collection.find_one({'chat_id': event.chat_id})
    if user is None:
        sender = await event.get_sender()
        user = {
            'chat_id': event.chat_id,
            'first_name': sender.first_name,
            'last_name': sender.last_name,
            'username': sender.username,
            'allow': False,
        }
        collection.update_one({'chat_id': event.chat_id}, {'$set': user}, upsert=True)
    if event.chat_id==int(os.environ['ADMIN_ID']):
        cmd = event.message.text.split(' ')
        if cmd[0]=='/a':
            collection.update_one({'chat_id': int(cmd[1])}, {'$set': {'allow': True}})
            await event.respond(f"User: {cmd[1]} allowed")
        elif cmd[0]=='/d':
            collection.update_one({'chat_id': int(cmd[1])}, {'$set': {'allow': False}})
            await event.respond(f"User: {cmd[1]} blocked")
    if event.chat_id!=int(os.environ['ADMIN_ID']) and not user.get('allow'):
        await event.respond("You don't have permission to use this bot")
    elif event.message.text in direct_reply.keys():
        await event.respond(direct_reply[event.message.text])
    elif event.message.text == '/immdel_on':
        await event.respond('Immediate deletion enabled.')
        collection.update_one({'chat_id': event.chat_id}, {'$set': {'immdel_on': True}})
    elif event.message.text == '/immdel_off':
        await event.respond('Immediate deletion disabled.')
        collection.update_one({'chat_id': event.chat_id}, {'$set': {'immdel_on': False}})
    elif event.message.text == '/cancel':
        await event.respond('Cancelled.')
        collection.update_one({'chat_id': event.chat_id}, {'$set': {'command': ''}})
    elif event.message.text == '/add_folder':
        await event.respond('Send the folder link')
        collection.update_one({'chat_id': event.chat_id}, {'$set': {'command': 'add_folder'}})
    elif user.get('command', '') == 'add_folder':
        folder = event.message.text.split(r'/s/')
        if len(folder)!=2:
            await event.respond('Invalid link')
            return
        collection.update_one({'chat_id': event.chat_id}, {'$set': {'nextcloud_domain': folder[0], 'folder_key': folder[1], 'command': ''}})
        await event.respond('Folder link add success')
    elif 'nextcloud_domain' in user and 'folder_key' in user:
        msg = None
        try:
            urls = find_all_urls(event.message)
            url_count = len(urls)
            if url_count == 0:
                return
            msg = await event.respond('wait...')
            for i, url in enumerate(urls):
                try:
                    await stream_download_to_nextcloud(url, user, msg, '' if url_count==1 else f'File {i+1} of {url_count}')
                except Exception as e2:
                    await msg.reply(f"Error: {e2}\nURL: {url}")
        except Exception as e:
            if msg:
                await msg.edit(f"Error: {e}")
            else:
                await event.reply(f"Error: {e}")
    else:
        await event.respond('Please /add_folder first')

with bot:
    bot.run_until_disconnected()
