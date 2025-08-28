import os,aiohttp,asyncio,urllib.parse,time,hashlib,logging
from telethon import TelegramClient,events,Button
from telethon.sessions import StringSession
from telethon.tl.types import MessageEntityUrl
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

load_dotenv(override=True)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
NEXTCLOUD_DOMAIN = os.getenv('NEXTCLOUD_DOMAIN', 'dms.uom.lk')
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
    last_percentage = 0
    last_edited_time = 0
async def prog_callback(desc, current, total, event, file_name, tk):
    percentage = current/total*100
    if tk.last_percentage+2 < percentage and tk.last_edited_time+5 < time.time():
        await event.edit(f"**{desc}ing**: {progress_bar(percentage)}\n**File Name**: {file_name}\n**Size**: {humanify(total)}\n**{desc}ed**: {humanify(current)}")
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
async def stream_download_to_nextcloud(download_url, folder_key, message, nc_domain=NEXTCLOUD_DOMAIN):
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
            file_org_name = urllib.parse.unquote(os.path.basename(parsed_url.path))
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
            tk = TimeKeeper()
            progress_callback = lambda c,t:prog_callback('Transload', c, t, message, file_org_name, tk)
            async with session.put(
                f"https://{nc_domain}/public.php/webdav/{file_org_name}",
                data=callback_pipe(resp.content.iter_chunked(1024), total, progress_callback),
                auth=aiohttp.BasicAuth(folder_key, ""),
                headers=up_headers
            ) as put_resp:
                if put_resp.status not in [200, 201, 204]:
                    raise Exception(f"Error-Code: {put_resp.status}")
                await message.edit(f"**File Name**: {file_org_name}\n**Size**: {humanify(total)}\nTransfer Successful ✅", buttons=[[Button.url("Open Folder 🔗", f'https://{nc_domain}/s/{folder_key}')]])
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
        }
        collection.update_one({'chat_id': event.chat_id}, {'$set': user}, upsert=True)
    if event.message.text.startswith('/start'):
        await event.respond('Hi')
        return
    if event.message.text.startswith('/add_folder'):
        folder_link = event.message.text.split(' ')
        if len(folder_link)!=2:
            await event.respond('Syntax to add folder link\n`/add_folder folder_link`')
        else:
            folder_key = folder_link[1].split('/s/')
            if len(folder_link)!=2:
                await event.respond('Invalid command')
                return
            collection.update_one({'chat_id': event.chat_id}, {'$set': {'folder_key': folder_key[1]}})
            await event.respond('Folder link add success')
        return
    elif 'folder_key' not in user:
        await event.respond('Please /add_folder first')
        return
    try:
        urls = find_all_urls(event.message)
        if len(urls) == 0:
            return
        msg = await event.respond('wait...')
        for url in urls:
            await stream_download_to_nextcloud(url, user['folder_key'], msg)
    except Exception as e:
        await event.respond(f"Error: {e}")

with bot:
    bot.run_until_disconnected()
