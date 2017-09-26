# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

# from scrapy import signals
import random

import time

import logging

logger = logging.getLogger(__name__)

import sys

from scrapy.http import HtmlResponse

from selenium import webdriver

from selenium.common.exceptions import UnexpectedAlertPresentException, WebDriverException

sys.path.append('..')

from libs.configs import USER_AGENTS


class RandomUserAgent(object):
    def __init__(self):
        self.agent = random.choice(USER_AGENTS)

    def process_request(self, request, spider):
        request.headers.setdefault('User-Agent', self.agent)


# class ProxyMiddleware(object):
#     def process_request(self, request, spider):
#         proxy = random.choice(PROXIES)
#         request.meta['proxy'] = "http://%s" % proxy['ip_port']


class JavaScriptMiddleware(object):
    def process_request(self, request, spider):
        global body
        if request.meta.has_key('PhantomJS'):
            # proxy = random.choice(PROXIES)
            # chrome_options = webdriver.ChromeOptions()
            # chrome_options.add_argument('--proxy-server=http://%s' % proxy['ip_port'])
            try:
                driver = webdriver.PhantomJS()
                driver.get(request.url)
                time.sleep(1)
                body = driver.page_source.encode('utf8')
                driver.quit()
            except UnexpectedAlertPresentException:
                body = self._retry(request.url)
            except WebDriverException:
                pass
            return HtmlResponse(request.url, body=body, encoding='utf-8', status=200)

    def _retry(self, url, times=0):
        driver = webdriver.Chrome()
        try:
            driver.get(url)
            time.sleep(1)
            page = driver.page_source.encode('utf8')
            return page
        except UnexpectedAlertPresentException:
            driver.switch_to.alert.accept()
            if times < 5:
                self._retry(url, times + 1)
                logger.debug("%s Gave up retrying %s：failed %d times",
                             time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), url, times)
