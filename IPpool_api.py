#!/usr/bin/env python
# -*- coding:utf-8 -*-

'''
代理IP池
'''

__author__ = 'Magic Yyx'

import pymongo
from fake_useragent import UserAgent
import threading
import requests
from bs4 import BeautifulSoup
import aiohttp
import asyncio
import time


class IPpool(object):
    def __init__(self):
        # 连接MongoDB
        self.__myClient = pymongo.MongoClient('mongodb://localhost:27017/')
        self.__myDB = self.__myClient['IPpool']
        self.__myCol = self.__myDB['pool']

        # 伪装User-Agent
        self.__ua = UserAgent()

        # 要爬取的代理网站
        self.__proxy_dict = {'kuaidaili': self.__kuaidaili, 'xicidaili': self.__xicidaili, 'yundaili': self.__yundaili}

        # lock
        self.__queue_lock = threading.Lock()

    # 从数据库取出IP
    def get_ip(self, n=0):
        # 按评分从高到低取，默认为0取出全部
        r = self.__myCol.find().sort('Score', -1).limit(n)
        ips = [ip['IP'] for ip in r]
        return ips

    # 删除数据库中小于一定评分的IP
    def delete_ip(self, score):
        delete_condition = {'Score': {'$lt': score}}
        self.__myCol.delete_many(delete_condition)

    # 验证IP是否可用
    def ip_test(self, ip, url_test, time_out):
        headers = {'User-Agent': self.__ua.random}
        proxies = {'http': 'http://' + ip, 'https': 'https://' + ip}
        try:
            response = requests.get(url=url_test, headers=headers, proxies=proxies, timeout=time_out)
            if response.status_code == 200:
                return True
            else:
                return False
        except Exception as e:
            return False

    # 查看当前数据库中IP的评分分布
    def ip_distribution(self):
        total_count = self.__myCol.estimated_document_count()
        print('Total Count:%d' % total_count)
        if total_count > 0:
            r = self.__myCol.distinct('Score')
            l = list(r)
            l.sort(reverse=True)
            for n in l:
                print('Score:%d  Count:%d  Ratio:%.1f%%' % (n, self.__myCol.count_documents({'Score': n}),
                                                            100 * self.__myCol.count_documents(
                                                                {'Score': n}) / total_count))

    # 执行
    def run(self, time_interval):
        # 一直循环下去
        while 1:
            self.__start_crawler()
            print('代理IP爬取完毕，开始进行检验...')
            self.__pool_test()
            print('检验完毕！休息%s分钟' % time_interval)
            time.sleep(time_interval * 60)

    # 爬取IP，多线程
    def __start_crawler(self):
        threads = []
        for proxy in self.__proxy_dict.keys():
            thread = threading.Thread(target=self.__proxy_dict[proxy], name=proxy)
            thread.start()
            threads.append(thread)
        for t in threads:  # 等待所有线程完成
            t.join()

    # 西刺代理
    def __xicidaili(self):
        page = 3  # 要爬取的页数
        ip_list = []  # 临时存储爬取下来的ip
        print('爬取西刺代理...')
        for p in range(page + 1):
            url = "https://www.xicidaili.com/nn/" + str(p + 1)
            html = self.__get_html(url)
            if html != "":
                soup = BeautifulSoup(html, "lxml")
                ips = soup.find_all("tr", class_="odd")
                for i in ips:
                    tmp = i.find_all("td")
                    ip = tmp[1].text + ':' + tmp[2].text
                    ip_list.append(ip)
                time.sleep(3)
            else:
                print("西刺代理获取失败！")
                break
        for item in ip_list:
            self.__queue_lock.acquire()
            self.__insertIP_to_MongoDB(item, 10)
            self.__queue_lock.release()

    # 快代理
    def __kuaidaili(self):
        page = 10  # 要爬取的页数
        ip_list = []  # 临时存储爬取下来的ip
        print('爬取快代理...')
        for p in range(page + 1):
            url = "https://www.kuaidaili.com/free/inha/{}/".format(p + 1)
            html = self.__get_html(url)
            if html != "":
                soup = BeautifulSoup(html, "lxml")
                ips = soup.select('td[data-title="IP"]')
                ports = soup.select('td[data-title="PORT"]')
                for i in range(len(ips)):
                    ip = ips[i].text + ':' + ports[i].text
                    ip_list.append(ip)
                time.sleep(3)
            else:
                print("快代理获取失败！")
                break
        for item in ip_list:
            self.__queue_lock.acquire()
            self.__insertIP_to_MongoDB(item, 10)
            self.__queue_lock.release()

    # 云代理
    def __yundaili(self):
        page = 5  # 要爬取的页数
        ip_list = []  # 临时存储爬取下来的ip
        print('爬取云代理...')
        for p in range(page + 1):
            url = "http://www.ip3366.net/free/?stype=1&page={}".format(p + 1)
            html = self.__get_html(url)
            if html != "":
                soup = BeautifulSoup(html, "lxml")
                ips_tmp = soup.find_all("tr")
                for i in range(1, len(ips_tmp)):
                    ips = ips_tmp[i].find_all("td")
                    ip = ips[0].text + ':' + ips[1].text
                    ip_list.append(ip)
                time.sleep(3)
            else:
                print('云代理获取失败！')
                break
        for item in ip_list:
            self.__queue_lock.acquire()
            self.__insertIP_to_MongoDB(item, 10)
            self.__queue_lock.release()



    # IP存入数据库
    def __insertIP_to_MongoDB(self, ip, score):
        # 重复ip不存储
        if self.__myCol.find_one({'IP': ip}) == None:
            self.__myCol.insert_one({'IP': ip, 'Score': score})

    # 获取页面源码
    def __get_html(self, url):
        headers = {'User-Agent': self.__ua.random}
        try:
            response = requests.get(url=url, headers=headers, timeout=5)
            if response.status_code == 200:
                return response.text
        except Exception as e:
            pass  # 获取源码失败
        # 如果不能访问，则使用ip池的代理ip进行尝试
        proxy_ips = self.get_ip()
        for proxy_ip in proxy_ips:
            proxies = {'http': 'http://' + proxy_ip, 'https': 'https://' + proxy_ip}
            try:
                response_proxy = requests.get(url=url, headers=headers, proxies=proxies, timeout=5)
                if response_proxy.status_code == 200:
                    return response_proxy.text
            except Exception as e:
                pass
        return ""  # 若所有代理均不能成功访问，则返回空字符串

    # 评分调整，验证成功的直接评分变为100，未验证成功的减1，评分为0的直接删除
    def __adjust_score(self, ip, myType):
        """
        :param ip:
        :param type: 1 100分，-1 减1分
        """
        if myType == 1:
            query_ip = {'IP': ip}
            new_value = {'$set': {'Score': 100}}
            self.__myCol.update_one(query_ip, new_value)
        elif myType == -1:
            query_ip = {'IP': ip}
            current_score = self.__myCol.find_one(query_ip)['Score']
            if current_score == 1:
                self.__myCol.delete_one(query_ip)
            else:
                new_value = {'$set': {'Score': current_score - 1}}
                self.__myCol.update_one(query_ip, new_value)

    # 测试IP是否可用
    async def __ip_test(self, url, headers, proxy):
        test_proxy = "http://" + proxy
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            try:
                async with session.get(url=url, headers=headers, proxy=test_proxy) as resp:
                    if resp.status == 200:
                        self.__adjust_score(proxy, 1)
                    else:
                        self.__adjust_score(proxy, -1)
            except Exception as e:
                self.__adjust_score(proxy, -1)

    def __pool_test(self):
        COUNTS = 100  # 每次测试100个ip
        proxy_ips = self.get_ip()
        test_url = "https://www.baidu.com"  # 可替换为要爬取的网址
        print('开始进行IP检验...')
        print('共{}个'.format(len(proxy_ips) + 1))
        for i in range(0, len(proxy_ips), COUNTS):
            tasks = [self.__ip_test(test_url, {"User-Agent": self.__ua.random}, ip) for ip in proxy_ips[i:i + COUNTS]]
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.wait(tasks))
            print("已测试{}个".format( COUNTS + i))
            time.sleep(5)
