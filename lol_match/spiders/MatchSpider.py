# coding=utf-8
import json

import redis

import scrapy

import re

import sys

import math

import MySQLdb

reload(sys)

sys.setdefaultencoding('utf-8')

from lol_match.items import LolMatchItem

from lol_match.items import LolMatchInfoItem

from scrapy.utils.project import get_project_settings

from twisted.enterprise import adbapi


def async_mysql():
    """
    mysql 异步连接池
    :return:
    """
    settings = get_project_settings()
    config = dict(
        host=settings.get('DB_HOST'),
        port=settings.get('DB_PORT'),
        user=settings.get('DB_USER'),
        passwd=settings.get('DB_PASSWORD'),
        db=settings.get('DB_NAME'),
        charset=settings.get('DB_CHARSET'),
    )
    return adbapi.ConnectionPool("MySQLdb", **config)


class MatchSpider(scrapy.Spider):
    name = 'lol_match'

    allowed_domains = ["apps.game.qq.com", "lpl.qq.com"]

    start_urls = [
        "http://apps.game.qq.com/lol/match/apis/searchBMatchInfo.php?r1=MatchList&page=1&pagesize=5&p2=1&p6=2"]

    need_crawl_teams = [
        1, 12, 6, 8, 4, 41, 42, 29, 7, 2, 9, 57, 43, 91, 106, 11, 37, 95, 55, 53, 28, 33, 32, 56, 292, 137, 24, 20, 227,
        224, 21, 222, 225, 226, 319, 255, 325, 320, 318, 255, 17, 256, 323, 257, 15, 324
    ]
    db_pool = async_mysql()

    match_set = set()

    match_info_set = set()

    def __init__(self):
        super(MatchSpider, self).__init__()
        self._fill()

    def _get_matches(self, txn):
        """
        填充比赛集合
        :param txn:
        :return:
        """
        txn.execute("SELECT b_match_id FROM lol_matches")
        records = txn.fetchall()
        if records:
            return records
        else:
            return None

    def _get_match_info(self, txn):
        """
        填充比赛详情集合
        :param txn:
        :return:
        """
        txn.execute("SELECT s_match_id FROM lol_match_infos")
        records = txn.fetchall()
        if records:
            return records
        else:
            return None

    def _fill_match_set(self, b_match_ids):
        """
        异步填充比赛集合
        :param b_match_ids:
        :return:
        """
        map(lambda b_match_id: self.match_set.add(b_match_id[0]) if b_match_id[0] else None, b_match_ids)

    def _fill_match_info_set(self, s_match_ids):
        """
        异步填充比赛详情集合
        :param s_match_ids:
        :return:
        """
        map(lambda s_match_id: self.match_info_set.add(s_match_id[0]) if s_match_id[0] else None, s_match_ids)

    def _fill(self):
        """
        异步填充
        :return:
        """
        self.db_pool.runInteraction(self._get_matches).addCallback(self._fill_match_set)
        self.db_pool.runInteraction(self._get_match_info).addCallback(self._fill_match_info_set)

    def filter_match(self, b_match_id=None, s_match_id=None):
        """
        爬虫过滤
        :param b_match_id:
        :param s_match_id:
        :return:
        """
        if b_match_id:
            fun = lambda v: v in self.match_set
            return fun(b_match_id)
        if s_match_id:
            fun = lambda x: x in self.match_info_set
            return fun(s_match_id)

    def parse(self, response):
        for team_number in self.need_crawl_teams:
            url = 'http://apps.game.qq.com/lol/match/apis/searchBMatchInfo.php?r1=MatchList&page=%d&pagesize=%d&p2=%d&p6=2' % (
                0, 5, team_number)
            yield scrapy.Request(url, self.parse_paginate, meta={'team_number': team_number})

    def parse_paginate(self, response):
        """
        解析每个战队的分页
        :rtype: object
        """
        body = re.sub(';$', '', response.body[response.body.find('=') + 1:])
        body = json.loads(body, encoding="utf8")
        if isinstance(body, dict) and body.has_key('msg'):
            if body['msg'].has_key('total'):
                max_page = int(math.ceil(float(body['msg']['total']) / 5))
                for i in range(1, max_page + 1):
                    url = 'http://apps.game.qq.com/lol/match/apis/searchBMatchInfo.php?r1=MatchList&page=%d&pagesize=%d&p2=%d&p6=2' % (
                        i, 5, int(response.meta['team_number']))
                    yield scrapy.Request(url, self.parse_match)

    def parse_match(self, response):
        # 响应信息
        """
        解析比赛信息
        :rtype: object
        """
        body = response.body
        # 去除无用数据
        search_index = body.index('=')
        if search_index != -1:
            body = body[search_index + 1:]
        body = re.sub(';$', '', body)
        # json转换为字典
        matches_dict = json.loads(body)
        item = LolMatchItem()
        # b match list
        if matches_dict.has_key('msg') and len(matches_dict['msg']['result']) > 0:
            for x in matches_dict['msg']['result']:
                if int(x['MatchStatus']) == 1:
                    continue
                else:
                    item['b_match_id'] = x['bMatchId']
                    item['b_match_name'] = x['bMatchName']
                    item['game_id'] = x['GameId']
                    item['game_name'] = x['GameName']
                    item['game_type_id'] = x['GameTypeId']
                    item['game_mode'] = x['GameMode']
                    item['game_type_name'] = x['GameTypeName']
                    item['game_proc_id'] = x['GameProcId']
                    item['game_proc_name'] = x['GameProcName']
                    item['left_team_number'] = x['TeamA']
                    item['left_team_score'] = x['ScoreA']
                    item['right_team_number'] = x['TeamB']
                    item['right_team_score'] = x['ScoreB']
                    item['match_date'] = x['MatchDate']
                    item['match_status'] = x['MatchStatus']
                    item['match_win'] = x['MatchWin']
                    # 去重
                    if int(x['bMatchId']) in self.match_set:
                        continue
                    else:
                        self.match_set.add(int(x['bMatchId']))
                        url = 'http://lpl.qq.com/es/stats.shtml?bmid=%d' % int(x['bMatchId'])
                        yield scrapy.Request(url, callback=self.parse_match_info, meta={"PhantomJS": True})
                        yield item

    def parse_match_info(self, response):
        """
        解析比赛详情链接后加入到请求队列
        :param response:
        :return:
        """
        for each in response.xpath('//ul[@id="smatch_bar"]/li'):
            s = each.xpath('@onclick').extract()[0]
            find_arr = re.findall(r'.*\((\d+),(\d+)\);$', s, re.DOTALL)
            # 去重
            if int(find_arr[0][0]) in self.match_info_set:
                continue
            else:
                self.match_info_set.add(int(find_arr[0][0]))
                url = 'http://apps.game.qq.com/lol/match/apis/searchMatchInfo_s.php?p0=%d&r1=MatchInfo' % int(
                    find_arr[0][0])
                yield scrapy.Request(url=url, callback=self.parse_match_info_response)

    def parse_match_info_response(self, response):
        """
        解析比赛详情
        :param response:
        :return:
        """
        # 页面响应信息
        body = response.body
        search_index = body.index('=')
        if search_index:
            body = body[search_index + 1:]
            body = re.sub(';$', '', body)
            # json转换为字典
        matche_info_dict = json.loads(body, encoding="utf8")
        item = LolMatchInfoItem()
        if isinstance(matche_info_dict['msg'], dict) and matche_info_dict['msg'].has_key('sMatchInfo'):
            # match info
            sMatchInfo = matche_info_dict['msg']['sMatchInfo']
            item['s_match_id'] = sMatchInfo['sMatchId'].encode('utf8')
            item['s_match_name'] = sMatchInfo['sMatchName'].encode('utf8')
            item['b_match_id'] = sMatchInfo['bMatchId'].encode('utf8')
            item['b_match_name'] = sMatchInfo['bMatchName'].encode('utf8')
            item['game_id'] = sMatchInfo['GameId'].encode('utf8')
            item['game_name'] = sMatchInfo['GameName'].encode('utf8')
            item['game_proc_id'] = sMatchInfo['GameProcId'].encode('utf8')
            item['game_proc_name'] = sMatchInfo['GameProcName'].encode('utf8')
            item['match_num'] = sMatchInfo['MatchNum'].encode('utf8')
            item['area_id'] = sMatchInfo['AreaId'].encode('utf8')
            item['battle_id'] = sMatchInfo['BattleId'].encode('utf8')
            item['left_team_number'] = sMatchInfo['TeamA'].encode('utf8')
            item['right_team_number'] = sMatchInfo['TeamB'].encode('utf8')
            item['match_status'] = sMatchInfo['MatchStatus'].encode('utf8')
            item['match_win'] = sMatchInfo['MatchWin'].encode('utf8')
            item['blue_team'] = sMatchInfo['BlueTeam'].encode('utf8')
        if isinstance(matche_info_dict['msg'], dict) and matche_info_dict['msg'].has_key('battleInfo'):
            # battle info
            battleInfo = matche_info_dict['msg']['battleInfo']
            item['battle_date'] = battleInfo['BattleDate'].encode('utf8')
            item['battle_time'] = battleInfo['BattleTime'].encode('utf8')
            item['battle_data'] = battleInfo['BattleData']
            item['s_updated'] = battleInfo['sUpdated'].encode('utf8')
        if isinstance(matche_info_dict['msg'], dict) and matche_info_dict['msg'].has_key('sMatchMember'):
            member_list = []
            for x in matche_info_dict['msg']['sMatchMember']:
                member_tuple = tuple([
                    x['bMatchId'], x['sMatchId'], x['BattleId'], x['AreaId'], x['TeamId'], x['MemberId'],
                    x['AccountId'], x['GameName'], x['Place'], x['ChampionId'], x['Game_K'], x['Game_D'],
                    x['Game_A'],
                    x['Game_M'], x['Game_W'], x['sUpdated'], x['iMVP'], x['iDVPNum']
                ])
                member_list.append(member_tuple)
            item['match_members'] = member_list
        yield item


class Duplicates(object):
    def __init__(self):
        self.redis_pool = redis.ConnectionPool(host="192.168.10.10", port=6379, db=0)
        self.redis_conn = self.connect()
        self.mysql_conn = MySQLdb.connect(host='127.0.0.1', port=33060, user='homestead',
                                          passwd='secret', db='caikes',
                                          charset='utf8')

    def connect(self):
        return redis.Redis(connection_pool=self.redis_pool)

    def in_b_match_set(self, b_match_id):
        print b_match_id
        return self.redis_conn.sadd('b_match_ids', b_match_id)

    def in_s_match_set(self, s_match_id):
        return self.redis_conn.sadd('s_match_ids', s_match_id)

    def in_matches(self, b_match_id):
        cur = self.mysql_conn.cursor()
        cur.execute("SELECT b_match_id FROM `lol_matches` WHERE b_match_id = %s" % str(b_match_id))
        record = cur.fetchone()
        return record

    def in_match_info(self, s_match_id):
        cur = self.mysql_conn.cursor()
        cur.execute("SELECT s_match_id FROM `lol_match_infos` WHERE s_match_id = %s" % str(s_match_id))
        record = cur.fetchone()
        return record
