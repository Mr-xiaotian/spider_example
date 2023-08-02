# -*- coding: utf-8 -*-
# 版本 2.00
# 作者：晓天
# 时间：2/8/2023
import re, sys, traceback
import asyncio
from time import sleep, time
from queue import Queue
from pprint import pprint
from tqdm import tqdm
import my_spider
import my_thread
import common_functions as cf


class MyFetcher(my_spider.Fetcher):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class MyDictoryThreader(my_thread.ExampleThreadManager):
    def get_args(self, obj: str) -> tuple:
        return (obj,)

    def process_result(self):
        return self.get_result_dict()


class MyFetchThreader(my_thread.ExampleThreadManager):
    def get_args(self, obj: list) -> tuple:
        return (obj,)

    def process_result(self, fetcher_queue):
        threader_content_dir = self.get_result_dict()
        return [(d, threader_content_dir[d]) for d in fetcher_queue]


class MyprocessThreader(my_thread.ExampleThreadManager):
    def get_args(self, obj: list) -> tuple:
        return (obj[1],)

    def process_result(self, process_queue):
        threader_content_dir = self.get_result_dict()
        return [(d[0], threader_content_dir[d]) for d in process_queue]


class CnkgraphSpider:
    def __init__(self):
        self.fetcher = MyFetcher(_wait_time=20, text_encoding="utf-8")
        self.cl = self.fetcher.cl
        self.saver = my_spider.Saver(
            cf.creat_folder(r"G:\Project\Spider\sou-yun\Cnkgraph"), False, 
            _wait_time = 15
        )
        self.suber = my_spider.Suber()
        self.dictory_threader = MyDictoryThreader(
            self.get_dictory, tqdm_desc="GetDictoryProcessing", show_progress=True
        )
        self.fetch_threader = MyFetchThreader(
            self.get_html, tqdm_desc="GetHtmlProcessing", show_progress=True
        )
        self.process_threader = MyprocessThreader(
            self.process_html, tqdm_desc="DealHtmlProcessing", show_progress=False
        )

        self.suber.sub_list += [('(<span>|</span>)', ''),
                                ("<span class='label'>", '### '),
                                ("<span class='title'>", '#### '),
                                ('<span class=".*?">', ''),
                                ("<span class='.*?'>", ''),
                                ('<br />', ''),
                                ("<span class='book'>", ''),
                                ("'pageImage' />", "'pageImage' />\n\n"),
                                ('//c./kanripoimgs', '//c.cnkgraph.com/kanripoimgs')
                                ]

        self.main_url = "https://cnkgraph.com"
        self.book_name = "UnknowBook"
        self.auther = "UnknowAuther"

        self.book_split = 100
        self.set_regular()

        self.book_list = []
        self.texts_list = []
        self.error_book_list = []

        self.file_error_list = []
        self.attend_dict = {}

    def set_regular(self):
        # 用于get_dictory
        self.re_dictory = re.compile('href="(/Book/.*?)">.*?<', re.S)

        # 用于process_dictory中提取章节id
        self.re_chapter_id = re.compile("https://cnkgraph.com/Book/(.*?)/(\d+)")

        # 用于process_html
        self.re_chapter_0 = re.compile('<nav class="nav">(.*?)</nav>', re.S)
        self.re_chapter_1 = re.compile('<a.*?href="(.*?)" class="nav-link link-dark".*?>(.*?)</a>')

        # 用于get_text
        self.re_text_0 = "<div class='kanripoPage.*?'>(.*?)</div>"
        self.re_text_1 = '(.*?)<br />'

    def set_book_id(self, rebook_id):
        self.book_id = rebook_id

    def set_chapter_range(self, ran):
        self.chapter_range = ran

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
        for name, page_num in cf.zip_range(self.book_list):
            print(f"{str(page_num)}:{name[1]}({name[0]})\n{name[2]} {name[3]}\n")

        # return self.book_list[book_id][0]

    async def get_book_async(self):
        def initialize_book_processing():
            """
            初始化书本信息
            """
            self.book_name = "UnknowBook"
            self.book_start_time = time()
            self.dictory_queue = Queue()
            # print(f"book id:", end="")

        def process_dictory():
            """
            处理self.get_dictory()得到的字典
            """
            temp_set = set(self.chapter_range)
            all_temp_set = set(self.chapter_range)
            dictory = set()

            while temp_set:
                self.dictory_threader.start(temp_set, "parallel")
                self.dictory_threader.handle_error()
                temp_dictory = self.dictory_threader.process_result()
                temp_set = set()
                for d in temp_dictory:
                    if set(temp_dictory[d]) <= all_temp_set:
                        continue
                    for di in temp_dictory[d]:
                        end_di = di.split('/')[-1]
                        if di not in all_temp_set:
                         if end_di.isdigit():
                             dictory.add(di)
                             continue
                         temp_set.add(di)  
                         all_temp_set.add(di)  

            # print(len(dictory))
            for d in dictory:
                self.dictory_queue.put(f'{self.main_url + d}')
            # cf.iprint(list(dictory))

        async def process_book_segments():
            """
            分批次处理全书，具体见self.get_book_async_segment()
            """
            try:
                while not self.dictory_queue.empty():
                    await self.get_book_async_segment()
            except Exception as e:
                await handle_error(e)

        async def handle_error(e):
            """
            处理错误，待完善
            """
            error = "".join(traceback.format_exception(*sys.exc_info()))
            print(error, file=sys.stderr)
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

        # 处理字典
        process_dictory()

        # 处理书本段落
        await self.fetcher.start_session()
        await process_book_segments()
        await self.fetcher.close_session()

        # 结束操作
        log_completion_time()

    async def get_book_async_segment(self):
        def initialize_segment_processing():
            self.texts_list = []

            self.fetcher_queue = [
                self.dictory_queue.get()
                for _ in range(min(self.book_split, self.dictory_queue.qsize()))
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
                self.chapter_list = self.process_threader.process_result(self.process_queue)
                self.process_queue = []

        async def finalize_segment_processing():
            for chapter in self.chapter_list:
                add_path_0 = self.re_chapter_id.search(chapter[0])
                add_path = f'{add_path_0.group(1)}({add_path_0.group(2)})'
                self.saver.set_add_path(f"{add_path}")

                file_list = []
                for c_index in chapter[1]:
                    text_id = c_index[0].split('/')[-1]
                    if '.' in text_id:
                        suffix_name = '.' + text_id.split('.')[-1]
                        file_list.append((c_index[1], c_index[0], suffix_name))
                    else:
                        text = f'### {c_index[1]}\n{self.get_text(c_index[0])}'
                        self.saver.download_text(
                            self.suber.sub_name(c_index[1]), text)
                if file_list:
                    await self.saver.download_urls(file_list)
    
        initialize_segment_processing()
        await process_fetcher_and_process_queues()
        await finalize_segment_processing()

    def get_dictory(self, copilot_url):
        dictory_url = f"{self.main_url}{copilot_url}"
        # print(dictory_url)

        dictory_content = self.fetcher.getHtml(dictory_url)
        dictory = self.re_dictory.findall(dictory_content)

        return dictory

    async def get_html(self, url):
        return await self.fetcher.getHtml_async_text(url)

    async def process_html(self, content):
        chapter_p_content = self.re_chapter_0.findall(content)

        temp_chapter_dictory_list = []
        chapter_dictory_list = []
        chemp_set = set()
        for cp in chapter_p_content[:]:
            temp_chapter_dictory_list += self.re_chapter_1.findall(cp)
        for cd in temp_chapter_dictory_list:
            if cd[0] in chemp_set:
                continue
            chapter_dictory_list.append(cd)
            chemp_set.add(cd[0])
        # cf.iprint([i[1] for i in chapter_dictory_list])

        return chapter_dictory_list
    
    def get_text(self, copilot_url):
        text_url = self.main_url + copilot_url
        content = self.fetcher.getHtml(text_url)
        p_content = ''.join(re.findall(self.re_text_0, content, re.S))
        #print(p_content)

        text_list = []
        text_list += re.findall(self.re_text_1, p_content, re.S)
        text_list = cf.list_removes(text_list, ' ')
        #print('\n'.join(text_list))
        
        return c.suber.clear_texts('\n'.join(text_list), {})

    def download_texts(self, texts_list=None, name_add=""):
        texts_list = self.texts_list if texts_list == None else texts_list

        self.book_name = self.suber.sub_name(self.book_name)

        texts = "\n".join(texts_list)
        texts = self.suber.clear_texts(texts, {})

        file_name = f"{self.book_name}{name_add}({self.book_id}).txt"
        self.saver.add_text(file_name, texts)


if __name__ == "__main__":
    c = CnkgraphSpider()
    c.set_book_id("32")
    c.set_chapter_range(['/Book', ])

    # c.dictory_threader.start([1, ], 'parallel')
    # c.dictory_threader.handle_error()
    # d = c.dictory_threader.process_result([1, ])
    # d = c.get_dictory('/Book')
    # print(d)

    asyncio.run(c.get_book_async())
    
    # c.clear_books()
    # a = c.search_book('宝可梦', interval_page = 1)