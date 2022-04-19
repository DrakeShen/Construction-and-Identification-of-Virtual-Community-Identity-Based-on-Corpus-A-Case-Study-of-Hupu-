# Construction and Identification of Virtual Community Identity Based on Corpus A Case Study of HupuFor sociolinguistic study

## Code of web spider
import requests
import pandas as pd
import time
import csv
import re
import datetime
import os
from lxml import etree


def is_null(list1):
    if list1:
        return list1[0]
    else:
        return ''


def is_null_num(list1):
    if list1:
        return int(
            list1[0].replace('万+', '0000').replace('\t', '').replace('\xa0', '').replace('\u3000', '').replace('\t\t',
                                                                                                               '').replace(
                '\n', '').replace('\r', '').strip().strip().replace(' ', '').replace('/', ''))
    else:
        return 0


def str_replace(list1):
    html_pattern1 = re.compile('style=.*?>')
    html_pattern2 = re.compile('width=.*?>')
    html_pattern3 = re.compile('height=.*?>')
    html_pattern4 = re.compile('image_type=.*?>')
    if len(list1) == 1:
        str1 = is_null(list1).replace('\t', '').replace('\xa0', '').replace('\u3000', '').replace('\t\t',
                                                                                                  '').replace(
            '\n', '').replace('\r', '').strip()
        html_list1 = html_pattern1.findall(str1)
        html_list2 = html_pattern2.findall(str1)
        html_list3 = html_pattern3.findall(str1)
        html_list4 = html_pattern4.findall(str1)
        html_list = html_list1 + html_list2 + html_list3 + html_list4
        for i in html_list:
            str1 = str1.replace(i, '')
        return str1
    elif len(list1) == 0:
        return ''
    else:
        list_new = []
        for i in list1:
            i = i.replace('\n', '').replace('\r', '').strip().replace('\n             ', '')
            if i == '' or i == '\n             ':
                pass
            else:
                list_new.append(i)
        str1 = ''.join(list_new).replace('\t', '').replace('\xa0', '').replace('\u3000', '').replace('\t\t',
                                                                                                     '').replace(
            '\r', '').strip()
        html_list1 = html_pattern1.findall(str1)
        html_list2 = html_pattern2.findall(str1)
        html_list3 = html_pattern3.findall(str1)
        html_list4 = html_pattern4.findall(str1)
        html_list = html_list1 + html_list2 + html_list3 + html_list4
        for i in html_list:
            str1 = str1.replace(i, '')
        return str1.replace("""'inline="0""", '').replace(r'\u200', '')


def split_time_ranges(from_time, to_time, frequency):
    # 拆分时间
    from_time, to_time = pd.to_datetime(from_time), pd.to_datetime(to_time)
    time_range = list(pd.date_range(from_time, to_time, freq='%sS' % frequency))
    if to_time not in time_range:
        time_range.append(to_time)
    time_range = [item.strftime("%Y-%m-%d %H:%M:%S") for item in time_range]
    time_ranges = []
    for item in time_range:
        f_time = item
        t_time = (datetime.datetime.strptime(item, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(seconds=frequency))
        if t_time >= to_time:
            t_time = to_time.strftime("%Y-%m-%d %H:%M:%S")
            time_ranges.append([f_time, t_time])
            break
        time_ranges.append([f_time, t_time.strftime("%Y-%m-%d %H:%M:%S")])
    return time_ranges


headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    # Requests sorts cookies= alphabetically
    'Cookie': 'smidV2=20210817173113f16eb5f77b5399a2646603f7bf311ae40074770f92ed47250; csrfToken=9JuB7yri9k93ZeU3dOEEe_PI; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2217f71b8ffa8114e-05f283e5df8d614-977173c-2073600-17f71b8ffa9f2a%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22%24device_id%22%3A%2217f71b8ffa8114e-05f283e5df8d614-977173c-2073600-17f71b8ffa9f2a%22%7D; Hm_lvt_df703c1d2273cc30ba452b4c15b16a0d=1646880425,1649225630; acw_tc=76b20ffe16492995152145512e7aa44ae0fe128143133b7317174d5c4d2634; Hm_lpvt_df703c1d2273cc30ba452b4c15b16a0d=1649299928; .thumbcache_33f5730e7694fd15728921e201b4826a=iczydjW9ocQG/Asz9Pw53m8I7ThLpqEo/+KIhaP/fjeoFlckyIZJ0MqhlKSLnPN+Jy4Cya2Y0fE6kY0faUpl8w%3D%3D',
    'Pragma': 'no-cache',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}



def save_csv(data_list, out_file):
    df = pd.DataFrame(data_list)
    # 通过判断时候有csv文件，来判断是否写表头，避免重复写表头
    is_writeheader = not os.path.exists(out_file)
    with open(out_file, 'a', encoding="utf-8-sig", newline="") as csvfile:

        writer = csv.DictWriter(csvfile, fieldnames=df.columns)
        if is_writeheader:
            writer.writeheader()  # 写入表头
            print('写入表头')

        for data in data_list:
            writer.writerow(data)


def spider(page):
    url = 'https://bbs.hupu.com/topic-daily-hot-{}'.format(page)
    print(url)
    response = requests.get(url, headers=headers)
    tree = etree.HTML(response.text)
    article_List = tree.xpath('//li[@class="bbs-sl-web-post-body"]')
    print("当前页:{},帖子数:{}".format(page, len(article_List)))
    for article in article_List:
        title = article.xpath('.//div[@class="post-title"]/a/text()')
        title = is_null(title)

        id = article.xpath('.//div[@class="post-title"]/a/@href')
        id = is_null(id).split('/')[-1].replace('.html','').replace('.html','')
        comment_view = article.xpath('.//div[@class="post-datum"]/text()')
        comment_view = str_replace(comment_view)
        comment_count = comment_view.split('/')[0]
        view_count = comment_view.split('/')[1]
        user_name = article.xpath('.//div[@class="post-auth"]/a/text()')
        user_name = is_null(user_name)
        user_id = article.xpath('.//div[@class="post-auth"]/a/@href')
        user_id = is_null(user_id).split('/')[-1]

        item1 = {}
        item1['id'] = id
        item1['title'] = title
        item1['comment_count'] = comment_count
        item1['view_count'] = view_count
        item1['user_name'] = user_name
        item1['user_id'] = user_id
        item1['url'] = 'https://bbs.hupu.com/{}.html'.format(id)
        get_info(item1)

    # next_page = tree.xpath('//li[@class=" hupu-rc-pagination-next"]/a/@href')
    # next_page = is_null(next_page)
    # if next_page:
    #     next_page = 'https://bbs.hupu.com/'+next_page
    #     get_comment(item,next_page)


def get_info(item):
    response_info = requests.get(item['url'], headers=headers)
    tree = etree.HTML(response_info.text)
    content = tree.xpath('//span[@class="post-user-comp-info-bottom-title"]/text()')
    content = str_replace(content)
    item['content'] = content
    time = tree.xpath('//span[@class="post-user-comp-info-top-time"]/text()')
    time = is_null(time)
    item['time'] = time
    # print(item)
    data_list = []
    data_list.append(item)
    save_csv(data_list, 'content.csv')


    comment_list = tree.xpath('//div[@class="post-reply-list "]')
    for comment in comment_list:
        comment_image = comment.xpath('.//div[@class="thread-content-detail"]//img/@src')
        content = comment.xpath('.//div[@class="reply-list-wrapper"]//text()')
        content = str_replace(content)
        item2 = {}
        item2['帖子id'] = item['id']
        item2['content'] = content
        user_id = comment.xpath('.//div[@class="user-base-info"]/a/@href')
        user_id = is_null(user_id).split('/')[-1]

        user_name = comment.xpath('.//div[@class="user-base-info"]/a/text()')
        user_name = is_null(user_name).split('/')[-1]

        time1 = comment.xpath('.//span[@class="post-reply-list-user-info-top-time"]/text()')
        time1 = is_null(time1).split('/')[-1]
        item2['user_id'] = user_id
        item2['user_name'] = user_name
        item2['time'] = time1
        item2['comment_image'] = comment_image
        # print(item2)
        data_list = []
        data_list.append(item2)
        save_csv(data_list, 'comment.csv')

    next_page = tree.xpath('//li[@class=" hupu-rc-pagination-next"]/a/@href')
    next_page = is_null(next_page)
    if next_page:
        next_page = 'https://bbs.hupu.com/'+next_page
        get_comment(item,next_page)

def get_comment(item,next_page):
    print(next_page)
    response_info = requests.get(next_page, headers=headers)
    tree = etree.HTML(response_info.text)
    comment_list = tree.xpath('//div[@class="post-reply-list "]')
    for comment in comment_list:
        content = comment.xpath('.//div[@class="reply-list-wrapper"]//text()')
        content = str_replace(content)
        item2 = {}
        item2['帖子id'] = item['id']
        item2['content'] = content
        user_id = comment.xpath('.//div[@class="user-base-info"]/a/@href')
        user_id = is_null(user_id).split('/')[-1]

        user_name = comment.xpath('.//div[@class="user-base-info"]/a/text()')
        user_name = is_null(user_name).split('/')[-1]

        time1 = comment.xpath('.//span[@class="post-reply-list-user-info-top-time"]/text()')
        time1 = is_null(time1).split('/')[-1]
        item2['user_id'] = user_id
        item2['user_name'] = user_name
        item2['time'] = time1
        # print(item2)
        data_list = []
        data_list.append(item2)
        save_csv(data_list, 'comment.csv')

    next_page = tree.xpath('//li[@class=" hupu-rc-pagination-next"]/a/@href')
    next_page = is_null(next_page)
    if next_page:
        next_page = 'https://bbs.hupu.com/'+next_page
        get_comment(item,next_page)



if __name__ == '__main__':
    for i in range(1,11):
        spider(i)
