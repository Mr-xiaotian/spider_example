import httpx, re, json
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from lxml import etree
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "cache-control": "max-age=0",
    "connection": "keep-alive",
    "cookie": "cangjieStatus_NZKPT2=true; cangjieConfig_NZKPT2=%7B%22status%22%3Atrue%2C%22startTime%22%3A%222022-10-20%22%2C%22endTime%22%3A%222025-04-24%22%2C%22orginHosts%22%3A%22kns.cnki.net%22%2C%22type%22%3A%22mix%22%2C%22poolSize%22%3A%2210%22%2C%22intervalTime%22%3A10000%2C%22persist%22%3Afalse%7D; Ecp_ClientId=o240822212600962103; SID_kns_new=kns2618107; Hm_lvt_dcec09ba2227fd02c55623c1bb82776a=1728904173; Hm_lpvt_dcec09ba2227fd02c55623c1bb82776a=1728904173; HMACCOUNT=CEF825C65830DFC2; Ecp_IpLoginFail=241014114.219.69.8; tfstk=gAUsKhtPodv13KKa2ZCEVuOJG10fa-_zBIGYZSLwMV3OhoNxLPo4QlrQc8P-BVWg_xnKp-G2ulWGGEMLhsYZ3mujh70tmlkqQxgbi81FUa7zs50mDTWPz9mOll0mHZuAzr3XD2XPUa7e-l3SETyahLlMdjDK6KHxk6iKMfgvDjexpDh-ad3xk-CIvflWDKL9XwdKavHxkqeY9XoHdjSsM5137Ik5lTG86vTvkyQmfYN90ELYRfi_k5HBbhzI1cMSYdRLkPeUMPzixgxroWr7hoedRFwb6uexQyBJlxroLDUTHaASpPNbRcqeBhhS5Ai8WDd65WiYAygaWtxuTWMtV2rFxOoq5RZoEcIhK-NIQ-Usvd6E3uPUWDwRLewmcles94IzuUk5Yj-XO0YjOY5COnxm1Vr-uyL7HvottfQPO69BmchnOYzVOnKZXXcTc61B4K1..",
    "host": "kns.cnki.net",
    "sec-ch-ua": '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0"
}

class ZhiSeleSpider:
    def __init__(self):
        options = Options()
        self.driver = webdriver.Edge(options=options)

    def init_list(self):
        self.literature_info = []
        self.error_list = []
    
    def init_page(self, search_term):
        search_url = 'https://www.cnki.net/'
        advsearch_url = 'https://kns.cnki.net/kns8s/AdvSearch?'
        self.driver.get(advsearch_url)
        # driver.maximize_window()

        # 选择"学位论文"
        # wait_and_click((By.XPATH, '//*[@id="ModuleSearch"]/div[2]/div/div/ul/li[2]/a'))

        # # 选择"学术辑刊"
        # self.wait_and_click((By.XPATH, '//*[@id="more"]'))
        # self.wait_and_click((By.XPATH, '//*[@id="ModuleSearch"]/div[2]/div/div/ul/li[11]/a'))

        # 等待搜索框变得可交互并输入搜索词
        input_selector = '#gradetxt > dd:nth-child(2) > div.input-box > input[type=text]'
        self.wait_and_click((By.CSS_SELECTOR, input_selector))
        search_box = self.driver.find_element(By.CSS_SELECTOR, input_selector)
        search_box.send_keys(search_term)

        # 点击"网络首发"
        # wait_and_click((By.CSS_SELECTOR, '#ModuleSearch > div.search-box > div > div.search-classify > div > div.grade-search-content > div.search-mainbox.is-off > div.search-middle > div.extend > div.extend-indent-labels > span.colorful-lable > label:nth-child(2)'))

        # # 选择"作者单位"
        # wait_and_click((By.XPATH, '//*[@id="DBFieldBox"]/div[1]'))
        # wait_and_click((By.XPATH, '//*[@id="DBFieldList"]/ul/li[9]/a'))
        # sleep(5)

        # 点击搜索按钮
        # wait_and_click((By.CSS_SELECTOR, '#ModuleSearch > div.search-box > div > div.search-classify > div > div.grade-search-content > div > div.search-mainbox.is-off > div.search-middle > div:nth-child(3) > div > input'))
        search_box.send_keys(Keys.ENTER)

        # 点击被引按钮
        # wait_and_click((By.XPATH, '//*[@id="CF"]'))

        sleep(2)
        # 找到包含"50"选项的div元素并点击
        self.wait_and_click((By.CSS_SELECTOR, '#perPageDiv > div'))
        self.wait_and_click((By.CSS_SELECTOR, 'li[data-val="50"]'))

    def get_ab_key(self, url):
        # 发送GET请求并解析页面内容
        response = httpx.get(url, headers=headers)
        result = {}

        # parser = etree.HTMLParser()
        # tree = etree.fromstring(response.text, parser)

        soup = BeautifulSoup(response.text, 'html.parser')

        # author_tag = tree.xpath('//*[@id="authorpart"]')
        # author = author_tag[0].xpath('string()').strip() if author_tag else ""
        # result['作者'] = re.sub(r"\s+", ";", author)

        # author_source_tag = tree.xpath('/html/body/div[2]/div[1]/div[3]/div/div[3]/div[1]/div/h3[2]')
        # author_source = author_source_tag[0].xpath('string()').strip() if author_source_tag else ""
        # result['作者单位'] = re.sub(r"\s+", ";", author_source)

        # 提取摘要
        abstract_tag = soup.find('span', {'id': 'ChDivSummary'})
        abstract = abstract_tag.get_text(strip=True) if abstract_tag else ""
        result['摘要'] = abstract

        # 提取关键词
        keywords_tag = soup.find('p', {'class': 'keywords'})
        keywords = keywords_tag.get_text(separator='; ', strip=True) if keywords_tag else ""
        result['论文关键词'] = keywords.replace(';;', ';')

        # # 提取基金资助 (如果需要的话，可以取消注释)
        # fund_tag = tree.xpath('//p[@class="funds"]')
        # fund = fund_tag[0].xpath('string()').strip() if fund_tag else ""
        # result['基金资助'] = re.sub(r"\s+", "", fund)

        # # # 提取 "DOI", "专辑", "分类号" 的内容
        # album_tag = soup.find('span', string='专辑：')
        # special_tag = soup.find('span', string='专题：')
        # classification_number_tag = soup.find('span', string='分类号：')
        # publis_time_tag = soup.find('span', string='在线公开时间：')

        # album = album_tag.find_next('p').text.strip() if album_tag else ""
        # special = special_tag.find_next('p').text.strip() if special_tag else ""
        # classification_number = classification_number_tag.find_next('p').text.strip() if classification_number_tag else ""
        # publis_time = publis_time_tag.find_next('p').text.strip() if publis_time_tag else ""

        # result['专辑'] = album
        # result['专题'] = special
        # result['分类号'] = classification_number
        # result['在线公开时间'] = publis_time

        return result

    def get_source_tag(self, url):
        response = httpx.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        journal_type = soup.find('p', class_='journalType journalType2')
        source_tag = journal_type.get_text(separator=' ',strip=True) if journal_type else ""
        return source_tag.replace(' ', '; ')

        
    def wait_and_click(self, locator, timeout=10):
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable(locator)
            )
            element.click()
            return element
        except TimeoutException:
            print(f"元素 {locator} 在 {timeout} 秒内未变为可点击状态")
        except NoSuchElementException:
            print(f"未找到元素 {locator}")


    def get_one_page(self, index):
        # 找到tbody元素
        try:
            tbody = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, 'tbody'))
            )
        except TimeoutException:
            print("页面加载超时,无法找到 tbody 元素")
            return []

        # 获取所有tr元素
        rows = tbody.find_elements(By.TAG_NAME, 'tr')

        literature_data = []
        error_list = []
        for literature in tqdm(rows, desc=f'提取第{index}页'):
            info = {}
            try:
                # 提取题名、URL、作者等信息
                title_tag = literature.find_element(By.CSS_SELECTOR, '.name a')
                title = title_tag.text or ''
                paper_link = title_tag.get_attribute('href') or ''
                author = literature.find_element(By.CSS_SELECTOR, '.author').text or ''
                # first_author = author.split(';')[0] if author else ''
                # author_num = len(author.split(';')) if author else 0
                source_item = literature.find_element(By.CSS_SELECTOR, '.source')
                source = source_item.text or ''
                # # surce_child = source_item.find_element(By.CSS_SELECTOR, 'a') or ''
                # # surce_url = surce_child.get_attribute('href') or '' if surce_child else ''
                date = literature.find_element(By.CSS_SELECTOR, '.date').text or ''
                # # data = literature.find_element(By.CSS_SELECTOR, '.data').text or ''
                # quote = literature.find_element(By.CSS_SELECTOR, '.quote').text or ''
                # download = literature.find_element(By.CSS_SELECTOR, '.download').text or ''

                info['论文题名'] = title
                info['作者'] = author
                # info['论文第一作者'] = first_author
                # info['一作外作者数量'] = author_num - 1
                info['辑刊（集刊）题名'] = source
                # info['paper_type'] = data
                info['发表时间'] = date
                # info['下载次数'] = download or 0
                # info['被引次数'] = quote or 0
                info['详情页链接'] = paper_link
                # info['Source URL'] = surce_url

                # html_tag = literature.find_element(By.CSS_SELECTOR, '.icon-html')
                # if html_tag:
                #     html_info_dict = self.get_html_info(html_tag)
                # else:
                #     continue

                # 获取摘要和关键词等
                page_result = self.get_ab_key(paper_link)
                # source_tag = get_source_tag(surce_url)
                # refer_text = self.get_refer_andsimilar_text(paper_link)

                # info['参考文献'] = refer_text

                info.update(page_result)
                # info.update(html_info_dict)
                literature_data.append(info)
                
            except Exception as e:
                title = title or '未知'
                error_list.append(f"第{index}页: 提取`{title}`信息时出错: {e}")

        return literature_data, error_list

    def change_search_condition(self, search_condition, wait_time = 2, first_year=None, end_year=None):
        self.wait_and_click((By.CLASS_NAME,'btn-unfold'))

        # 输入内容
        search_xpath_1 = '//*[@id="gradetxt"]/dd[3]/div[2]/input'
        search_box_1 = self.wait_and_click((By.XPATH, search_xpath_1))
        search_box_1.clear()
        search_box_1.send_keys(search_condition)

        # start_year_xpath = '//*[@id="ModuleSearch"]/div[1]/div/div[2]/div/div[1]/div/div[1]/div[2]/div[2]/div[2]/div[1]/div[1]/div/input'
        # wait_and_click((By.XPATH, start_year_xpath))
        # search_box = driver.find_element(By.XPATH, start_year_xpath)
        # search_box.send_keys(str(first_year)) if first_year else None

        # end_year_xpath = '//*[@id="ModuleSearch"]/div[1]/div/div[2]/div/div[1]/div/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div/input'
        # wait_and_click((By.XPATH, end_year_xpath))
        # search_box = driver.find_element(By.XPATH, end_year_xpath)
        # search_box.send_keys(str(end_year)) if end_year else None

        # 点击搜索按钮
        search_box_1.send_keys(Keys.ENTER)

        sleep(wait_time)
        # 找到包含"50"选项的div元素并点击
        # self.wait_and_click((By.CSS_SELECTOR, '#perPageDiv > div'))
        # self.wait_and_click((By.CSS_SELECTOR, 'li[data-val="50"]'))

    def reverse_time_index(self):
        self.wait_and_click((By.CSS_SELECTOR, '#PT'))
        sleep(1)

    def get_some_page(self, pagr_num):
        for num in range(pagr_num):
            page_data, page_error = self.get_one_page(num+1)
            if not page_data:
                break
            self.literature_info += page_data
            self.error_list += page_error

            try:
                element = self.driver.switch_to.active_element
                element.send_keys(Keys.ARROW_RIGHT)
                sleep(3)
            except Exception as e:
                print(f"发送右方向键失败: {e}")
                break

    def roll_down(self):
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        while True:
            # 滚动到底部
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # 等待页面加载新内容
            sleep(2)
            # 获取新的滚动高度
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            # 如果没有新内容，则退出循环
            if new_height == last_height:
                break
            last_height = new_height

    def get_refer(self):
        journal_list = self.get_refer_text('期刊', '#nxgp-kcms-data-ref-references-journal')
        journal_w_list = self.get_refer_text('国际期刊', '#nxgp-kcms-data-ref-references-journal-w')
        crldeng_list = self.get_refer_text('中外文题录', '#nxgp-kcms-data-ref-references-crldeng')
        book_text = self.get_refer_text('图书', '#nxgp-kcms-data-ref-references-book')
        newpaper_text = self.get_refer_text('报纸', '#nxgp-kcms-data-ref-references-newpaper')

        refer_list = [journal_list, journal_w_list, crldeng_list, book_text, newpaper_text]
        refer_list = ([i for i in refer_list if len(i) > 1])
        return "\n".join(refer_list)

    def get_refer_text(self, name, refer_id):
        journal_list = [f"{name}:"]
        while True:
            try:
                journal_tag = self.driver.find_element(By.CSS_SELECTOR, f'{refer_id} > ul')
                journal_list.append(journal_tag.text)

                next = self.driver.find_element(By.CSS_SELECTOR, f'{refer_id}-page > a.next')
                next.click()
                sleep(1)
            except:
                break
        return "\n".join(journal_list)

    def get_refer_andsimilar_text(self, url):
        # 打开一个新的标签页
        self.driver.execute_script("window.open('');")
        sleep(2)

        # 切换到新的标签页
        self.driver.switch_to.window(self.driver.window_handles[-1])
        self.driver.get(url)

        # 在新的标签页中进行操作
        self.roll_down()
        
        refer_text = self.get_refer()
        sleep(2)

        # 关闭新的标签页
        self.driver.close()

        # 切换回最初的标签页
        self.driver.switch_to.window(self.driver.window_handles[0])

        return refer_text
    
    def get_html_info(self, tag):
        tag.click()
        sleep(2)
        self.driver.switch_to.window(self.driver.window_handles[-1])
        results = {}

        try:
            school_tag = self.driver.find_element(By.XPATH, '//*[@id="1000002"]')
            school = school_tag.text if school_tag else ''

            fund_tage = self.driver.find_element(By.XPATH, '//*[@id="1000005"]')
            fund = fund_tage.text if fund_tage else ''

            fund_class_dict = {'国家社科': "国家级", "省社科": "省部级", "教育部社科": "省部级", 
                                "学校项目": "省部级以下", "学会": "专业行业企业", "公司": "企业"}
            
            first_fund = fund.split(';')[0]
            results['项目级别'] = ''
            for key, value in fund_class_dict.items():
                if key in first_fund:
                    results['项目级别'] = value
                    break

            keyword_tag = self.driver.find_element(By.XPATH, '//*[@id="paperRead"]/div/div/div/div[3]/div[3]')
            keyword = keyword_tag.text if keyword_tag else ''

            author_info_tag = self.driver.find_element(By.XPATH, '//*[@id="1000008"]')
            author_info = author_info_tag.text if author_info_tag else ''

            results['第一作者单位'] = school
            results['论文依托项目名称'] = fund.replace("基金：", "").strip()
            results['论文关键词'] = keyword.replace("关键词：", "").strip()
            results['一作学历职称'] = author_info.split(";")[0].replace("作者简介：", "").strip()
        
        finally:
            # 关闭新的标签页
            self.driver.close()

            # 切换回最初的标签页
            self.driver.switch_to.window(self.driver.window_handles[0])

            sleep(1)

            return results
    
    def get_literature_info(self):
        return self.literature_info
    
    def get_error_list(self):
        return self.error_list

    def save_info_to_csv(self, name, index_list):
        df = pd.DataFrame(clean_data(self.literature_info), columns=index_list)
        print(f"{len(df)}条数据")

        # # 按发表时间降序排序
        # df = df.sort_values(by='发表时间', ascending=False)
        # # 移除发表时间列（若不需保留）
        # df = df.drop(columns=['发表时间'])

        # df = df.drop_duplicates()
        # df['被引次数'] = df['被引次数'].replace('', pd.NA).fillna(0)
        # df['下载次数'] = df['下载次数'].replace('', pd.NA).fillna(0)

        df.to_csv(f'{name}.csv', index=False, encoding='utf-8-sig')
        self.init_list()
        return df.head(5)
    
    def save_cookie(self):
        # 获取当前所有cookies
        cookies = self.driver.get_cookies()

        # 将cookies保存到JSON文件
        with open('cookies.json', 'w') as file:
            json.dump(cookies, file, indent=4)  # indent参数使文件更易读

    def load_cookie(self):
        # 从JSON文件中读取Cookie
        with open('cookies.json', 'r') as file:
            cookies = json.load(file)

        # 逐个添加Cookie到浏览器
        for cookie in cookies:
            # 处理可能的expiry类型问题（JSON可能将expiry转为float）
            if "expiry" in cookie:
                cookie["expiry"] = int(cookie["expiry"])  # 转换为整数
            self.driver.add_cookie(cookie)

        # 刷新页面或跳转到需要Cookie的页面
        # self.driver.refresh()  # 或 driver.get("https://example.com/dashboard")

def clean_data(literature_info):
    new_literature_info = []

    for info in literature_info:
        if info in new_literature_info:
            continue
        new_literature_info.append(info)

    return new_literature_info