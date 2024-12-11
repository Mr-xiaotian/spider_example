# -*- coding: utf-8 -*-
# 版本 1.00
# 作者：晓天
# 时间：23/9/2023
import re, sys, traceback
import asyncio
import subprocess
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import base64
from time import sleep, time
from queue import Queue
from pprint import pprint
from tqdm import tqdm
from json import loads, dumps
from pickle import dump, load
from bs4 import BeautifulSoup, NavigableString, Tag
from CelestialVault.src.tools import creat_folder
from CelestialVault.src.instances import Saver, Suber, Fetcher, ThreadManager, ExampleThreadManager


class MyFetcher(Fetcher):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class MyFetchThreader(ExampleThreadManager):
    def get_args(self, obj: list) -> tuple:
        return (obj[1],)

    def process_result(self, fetcher_queue):
        threader_content_dir = self.get_result_dict()
        return [(d[0], threader_content_dir[d]) for d in fetcher_queue]


class MyprocessThreader(ExampleThreadManager):
    def get_args(self, obj: list) -> tuple:
        return (obj[0], obj[1])

    def process_result(self, fetcher_queue):
        threader_content_dir = self.get_result_dict()
        return [threader_content_dir[d] for d in fetcher_queue]


class Hlj04Spider:
    def __init__(self):
        self.fetcher = MyFetcher(_wait_time=20, text_encoding="utf-8")
        self.cl = self.fetcher.cl
        self.saver = Saver(
            creat_folder(r"G:\Project\Spider\hlj04\download")
        )
        self.suber = Suber()
        self.fetch_threader = MyFetchThreader(
            self.get_html, tqdm_desc="GetHtmlProcessing", show_progress=True
        )
        self.process_threader = MyprocessThreader(
            self.process_html, tqdm_desc="DealHtmlProcessing", show_progress=False
        )

        self.suber.sub_list += [
            ('(?<!－|#|＊|◆|\*|=|＝|…|～|。|]|』|」|】|》|\)|）|!|！|\?|？|—|”|"|_|\.)\n', ''),
            ('\n', '\n'*2), ('<br/>', ''), ]

        self.main_url = "http://www.hlj04.com"
        self.book_name = "UnknowBook"
        self.auther = "UnknowAuther"

        self.book_split = 100
        self.set_regular()

        self.book_list = []
        self.texts_list = []
        self.error_book_list = []
        self.error_text = ""
        self.attend_dict = {}

    def set_regular(self):
        # 用于get_book_async_segment中提取章节id
        self.re_chapter_id = re.compile("/readbook/\d+/(\d+).html")

        # 用于process_html
        # re_chapter_title = "<title>(.*?)_"
        self.re_html = re.compile('</blockquote>(.*?)<p><a class="content-file"', re.S)
        self.re_se_name = re.compile('itemprop="headline" content="(.*?)"', re.S)
        self.re_video_name = re.compile('([0-9 a-z]*?).m3u8', re.S)

        # get_directory
        self.re_directory = re.compile('itemprop="headline" content="(.*?)"', re.S)

    def set_book_id(self, rebook_id):
        self.book_id = rebook_id

    def set_directory_range(self, ranges):
        self.directory_range = ranges

    def get_book_list(self, html):
        re_book_list = '<a href="/(.*?)/"><p class="title">(.*?)</p>'
        ori_book_lists = re.findall(re_book_list, html, re.S)
        book_list = []

        num = 0
        for book in ori_book_lists:
            book_list.append(book[0])
            print(f"{num}:{book[1]}")
            num += 1

        return book_list

    def search_book(self, search_key, interval_page=5, start_page=1):
        search_url = self.main_url + "s.php"
        re_search_0 = '<a class="name" href="/\d{1,3}/(\d{3,6})/">(.*?)</a>.*?'
        re_search_1 = '(作者：.*?) <span class="words">(字数：\d+)</span>'
        re_search = re_search_0 + re_search_1
        re_num = "\(第\d+/(\d+)页\)当前\d+条/页"
        re_ = "<title>(.*?)下载"

        flag = True
        for page in range(start_page, start_page + interval_page):
            try:
                json_date = {
                    "type": "articlename",
                    "s": "Ե",
                    "page": page,
                    "accept-charset": "gbk",
                }
                search_content = self.fetcher.postHtml(search_url, data=json_date)
                print(re.search(re_, search_content, re.S).group(1))

            except Exception as e:
                print("第" + str(page) + "页爬取失败")
                print(e)
                break

            search_list = re.findall(re_search, search_content, re.S)
            if page == start_page:
                page_num = re.search(re_num, search_content, re.S).group(1)
                print(f"开始搜索[{search_key}]，总共可检索{page_num}页:")

            if search_list == []:
                break

            print("第" + str(page) + "页爬取完成")
            self.book_list += search_list
            sleep(2)

        # pprint(self.book_list)
        for page_num,name in enumerate(self.book_list):
            print(f"{str(page_num)}:{name[1]}({name[0]})\n{name[2]} {name[3]}\n")

        # return self.book_list[book_id][0]

    def get_books(self, book_list=None):
        book_list = [i[0] for i in self.book_list] if book_list == None else book_list
        self.error_book_list = []
        self.error_text = ""

        print(f"列表已更新，总共{len(book_list)}本书，现在开始下载.")
        for num, book in enumerate(book_list):
            book = str(book)
            if "/" in book:
                self.book_id = book
            else:
                self.book_id = self.set_book_id(book)
            self.get_book()
            print(f"(num:{num+1}/{len(book_list)})")
            pass

        if self.error_book_list != []:
            print("\n下载完成，其中开启断点保护的文件有:")
            pprint(self.error_book_list)
            print(self.error_text)

    async def get_book_async(self):
        def initialize_book_processing():
            """
            初始化书本信息
            """
            self.book_name = "UnknowBook"
            self.book_start_time = time()
            self.directory_queue = Queue()
            print(f"book id:", end="")

        def process_directory(directory):
            """
            处理self.get_directory()得到的字典
            """
            for d in directory:
                self.directory_queue.put(
                    (f'{d[0]}({d[1]})', d[1])
                    )
            # print(f"{self.book_name}({self.book_id})")
            # cf.iprint(directory)
            # directory_text = "\n".join(
            #     [f"[{i[1]}({i[0]})](#{i[1]}({i[0]}))" for i in directory]
            # )
            # self.texts_list.append(self.information + '\n\n目录:')
            # self.texts_list.append(directory_text + '\n')

        async def process_book_segments():
            """
            分批次处理全书，具体见self.get_book_async_segment()
            """
            try:
                while not self.directory_queue.empty():
                    await self.get_book_async_segment()
            except Exception as e:
                await handle_error(e)

        async def handle_error(e):
            """
            处理错误，待完善
            """
            error = "".join(traceback.format_exception(*sys.exc_info()))
            print(error, file=sys.stderr)
            print("\n出错！开始下载错误日志", file=sys.stderr)
            # 更多错误处理代码...
            # self.error_book_list.append(self.book_id)
            # self.book_name = self.suber.sub_name(self.book_name)

            # now_time = cf.get_now_time()
            # file_name = f"{self.book_name}(错误日志)({self.book_id})({now_time})"
            # self.saver.download_text(
            #     file_name,
            #     f"当前错误:\n{error}\n\n"
            #     + f"线程错误:\n{'/'.join([str(el) for el in self.fetch_threader.error_list])}",
            #     suffix_name=".txt",
            # )

        def log_completion_time():
            """
            打印完成信息
            """
            print(f"\n{self.book_name}下载完成, 总用时{time() - self.book_start_time:.2f}s")
            sleep(3)

        # 初始化操作
        initialize_book_processing()
        await self.fetcher.start_session()

        # 处理字典
        directory = self.get_directory()
        process_directory(directory)

        # 处理书本段落
        await process_book_segments()

        # 结束操作
        await self.fetcher.close_session()
        log_completion_time()

    async def get_book_async_segment(self):
        def initialize_segment_processing():
            self.texts_list = []

            self.fetcher_queue = [
                self.directory_queue.get()
                for _ in range(min(self.book_split, self.directory_queue.qsize()))
            ]
            self.process_queue = []

        async def process_fetcher_and_process_queues():
            while self.fetcher_queue or self.process_queue:
                await handle_fetcher_queue()
                await handle_process_queue()

        async def handle_fetcher_queue():
            if self.fetcher_queue:
                await self.fetch_threader.start_async(self.fetcher_queue)
                self.fetch_threader.handle_error()
                result_list = self.fetch_threader.process_result(self.fetcher_queue)
                self.process_queue.extend(result_list)
                self.fetcher_queue = []

        async def handle_process_queue():
            if self.process_queue:
                await self.process_threader.start_async(self.process_queue)
                self.process_threader.handle_error()
                self.chapter_list = self.process_threader.process_result(
                    self.process_queue
                )
                self.process_queue = []

        def finalize_segment_processing():
            for file_name, (text, img_list, video_list) in self.chapter_list:
                self.saver.set_add_path(file_name)
                self.saver.download_text(file_name, text)

                for img in tqdm(img_list, desc=f'ImageDownload {file_name}: '):
                    img_content = self.fetcher.cl.get(img[1]).content
                    try:
                        img_content_true = self.decrypt(img_content)
                        self.saver.download_content(img[0], img_content_true, '.jpg')
                    except:
                        self.error_list.append(file_name)

                for video in tqdm(video_list):
                    command = ['ffmpeg',
                    '-protocol_whitelist', 'file,http,https,tcp,tls,crypto',
                    '-i', video,
                    '-c', 'copy', self.saver.get_path(self.re_video_name.search(video).group(1), '.mp4')]
                    subprocess.run(command)

        initialize_segment_processing()
        await process_fetcher_and_process_queues()
        finalize_segment_processing()

    def get_directory(self):
        # directory = [
        #     ('日本痴汉犯罪实录 FC2暗黑王子作品全集收录（恢复更新 更至14期）', 9031),
        # ]

        # with open('directory(3970-43122).pickle', 'rb') as f:
        #     directory_pickle = load(f)
        # directory = directory_pickle[2715:]
        # [directory_pickle[i] for i in self.directory_range]

        for i in self.directory_range:
            directory_url = f'{self.main_url}/archives/{i}/'
            directory_content = self.fetcher.getHtml(directory_url)
            directory_name = self.re_directory.search(directory_content)
            if directory_name != None:
                directory.append((directory_name.group(1), i))
        
        return directory

    async def get_html(self, url):
        url = f'http://www.hlj04.com/archives/{url}/'
        return await self.fetcher.getText_async(url)

    async def process_html(self, name, content):
        html = self.re_html.search(content).group(1)
        # 创建BeautifulSoup对象
        soup = BeautifulSoup(html, 'html.parser')

        # 初始化一个空字符串来保存Markdown内容
        self.md_content = ""
        self.video_list = []
        self.img_list = []

        # 调用递归函数开始遍历
        self.traverse(soup)

        # 使用markdownify库处理任何剩余的HTML标签，并将其转换为Markdown格式（如果有必要）
        # md_content = markdownify.markdownify(self.md_content, strip=['p'])
        # md_content = self.md_content.replace('\\_', '_')

        file_name = self.suber.sub_name(name)
        text = f'# {file_name}\n\n{self.md_content}'
        # text = self.suber.clear_texts(text)

        return file_name, (text, self.img_list[:], self.video_list[:])
    
    def traverse(self, element):
        # 定义一个递归函数来遍历所有元素
        if element:
            if isinstance(element, NavigableString):  # 如果元素是文本
                text = element.strip()
                if text:  # 如果文本非空
                    self.md_content += f"{text}\n\n"  # 添加文本到Markdown内容
            elif isinstance(element, Tag):  # 如果元素是一个HTML标签
                if element.name == 'img'and element.get('title') != None:  # 如果元素是<img>标签
                    img_src = element.get('data-xkrkllgl')
                    img_title = element.get('title').replace(':', '_')
                    if img_src:
                        self.md_content += f"![{img_src}]({img_title})\n\n"  # 添加图片到Markdown内容
                        self.img_list.append((img_title, img_src))
                elif 'dplayer' in element.get('class', []):  # 如果是视频
                    video_config = element.get('data-config')
                    if video_config:
                        config_json = loads(video_config)
                        video_url = config_json.get('video', {}).get('url', '')
                        video_name = self.re_video_name.search(video_url).group(1)

                        # 添加视频链接到Markdown内容
                        self.md_content += f'<video controls src="{video_name}.mp4" width="480" height="320">{video_url}</video>\n\n'
                        self.video_list.append(video_url)
                # 递归遍历所有子元素
                for child in element.children:
                    self.traverse(child)

    def decrypt(self, encrypted_data, key='f5d965df75336270', iv='97b60394abc2fbe1'):
        # Create AES cipher object
        cipher = AES.new(key.encode(), AES.MODE_CBC, iv.encode())
        
        # Decrypt the data
        decrypted_data = cipher.decrypt(encrypted_data)
        
        # Unpad the decrypted data
        decrypted_data = unpad(decrypted_data, AES.block_size)
        
        return decrypted_data


if __name__ == "__main__":
    h = Hlj04Spider()
    h.set_directory_range(range(1000, 2741))
    h.error_list = ['23880', '35114', '35114']

    # t = h.suber.clear_texts(h.get_texts('47843664'), {})
    # print(t)
    # d = h.get_directory()
    # print(d)
    # asyncio.run(h.get_book_async())
    # print(h.error_list)
    # w.clear_books()
    # a = w.search_book('宝可梦', interval_page = 1)

    content = h.fetcher.getText('http://www.hlj04.com/archives/35114/')
    print(content)
    # a = asyncio.run(h.process_html('test', content))
    # print(a)
    # file_name, (text, img_list, video_list) = a
    # for img in img_list:
    #     print(img[0])
    #     img_content = h.fetcher.cl.get(img[1]).content
    #     img_content_true = h.decrypt(img_content)