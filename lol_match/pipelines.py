# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sys

import MySQLdb

from scrapy.exceptions import DropItem

from lol_match.items import LolMatchItem

from lol_match.items import LolMatchInfoItem

sys.path.append("..")


from twisted.enterprise import adbapi


class LolMatchPipeline(object):
    def __init__(self, conn):
        self.conn = conn
        self.cur = self.conn.cursor()

    @classmethod
    def from_settings(cls, settings):
        MYSQL_CONFIG = dict(
            host=settings['DB_HOST'],
            port=settings['DB_PORT'],
            user=settings['DB_USER'],
            passwd=settings['DB_PASSWORD'],
            db=settings['DB_NAME'],
            charset=settings['DB_CHARSET'],
        )
        conn = MySQLdb.connect(**MYSQL_CONFIG)
        return cls(conn)

    def process_item(self, item, spider):
        # 比赛
        if isinstance(item, LolMatchItem):
            try:
                sql = "INSERT INTO `lol_matches`(`b_match_id`,`b_match_name`," \
                      "`game_id`,`game_name`,`game_type_id`,`game_mode`,`game_type_name`,`game_proc_id`,`game_proc_name`," \
                      "`left_team_number`,`left_team_score`,`right_team_number`,`right_team_score`,`match_date`,`match_status`,`match_win`) " \
                      "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                value = [item['b_match_id'], item['b_match_name'], item['game_id'], item['game_name'],
                         item['game_type_id'], item['game_mode'], item['game_type_name'], item['game_proc_id'],
                         item['game_proc_name'], item['left_team_number'], item['left_team_score'],
                         item['right_team_number'], item['right_team_score'], item['match_date'], item['match_status'],
                         item['match_win']]
                self.cur.execute(sql, value)
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
        # 比赛详情
        if isinstance(item, LolMatchInfoItem):
            try:
                info_sql = "INSERT INTO `lol_match_infos`(`s_match_id`,`s_match_name`,`b_match_id`,`b_match_name`," \
                           "`game_id`,`game_name`,`game_proc_id`,`game_proc_name`,`match_num`,`area_id`," \
                           "`battle_id`,`left_team_number`,`right_team_number`,`match_status`,`match_win`,`blue_team`) " \
                           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                info_val = [
                    item['s_match_id'], item['s_match_name'], item['b_match_id'], item['b_match_name'], item['game_id'],
                    item['game_name'], item['game_proc_id'], item['game_proc_name'],
                    item['match_num'], item['area_id'], item['battle_id'], item['left_team_number'],
                    item['right_team_number'], item['match_status'], item['match_win'], item['blue_team'],
                ]
                battle_sql = "INSERT INTO `lol_match_battles`(`battle_id`,`battle_date`,`battle_time`,`area_id`,`battle_data`,`s_updated`) " \
                             "VALUES (%s,%s,%s,%s,%s,%s)"
                battle_val = [
                    item['battle_id'], item['battle_date'], item['battle_time'], item['area_id'], str(item['battle_data']),
                    item['s_updated']
                ]
                member_sql = "INSERT INTO `lol_match_players`(`b_match_id`,`s_match_id`,`battle_id`," \
                             "`area_id`,`team_id`,`member_id`,`account_id`,`game_name`,`place`,`champion_id`," \
                             "`game_k`,`game_d`,`game_a`,`game_m`,`game_w`,`s_updated`,`i_mvp`,`i_dvp_num`) " \
                             "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cur.execute(info_sql, info_val)
                self.cur.execute(battle_sql, battle_val)
                self.cur.executemany(member_sql, item['match_members'])
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()


class AsyncDuplicatesPipeline(object):
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.match_set = set()
        self.match_info_set = set()
        self._fill()

    @classmethod
    def from_settings(cls, settings):
        MYSQL_CONFIG = dict(
            host=settings['DB_HOST'],
            port=settings['DB_PORT'],
            user=settings['DB_USER'],
            passwd=settings['DB_PASSWORD'],
            db=settings['DB_NAME'],
            charset=settings['DB_CHARSET'],
        )
        # redis_pool = redis.ConnectionPool(host=settings['REDIS_HOST'], port=settings['REDIS_PORT'],
        #                                   db=settings['REDIS_DB'])
        db_pool = adbapi.ConnectionPool("MySQLdb", **MYSQL_CONFIG)
        return cls(db_pool)

    def _get_matches(self, txn):
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


    def process_item(self, item, spider):
        # match
        if isinstance(item, LolMatchItem):
            if int(item['b_match_id']) in self.match_set:
                raise DropItem("Duplicate item found: %s" % item)
            else:
                self.match_set.add(int(item['b_match_id']))
                return item
        # match info
        if isinstance(item, LolMatchInfoItem):
            if int(item['s_match_id']) in self.match_info_set:
                raise DropItem("Duplicate item found: %s" % item)
            else:
                self.match_info_set.add(int(item['s_match_id']))
                return item


class MySqlAsyncPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        config = dict(
            host=settings['DB_HOST'],
            port=settings['DB_PORT'],
            user=settings['DB_USER'],
            passwd=settings['DB_PASSWORD'],
            db=settings['DB_NAME'],
            charset=settings['DB_CHARSET'],
        )
        dbpool = adbapi.ConnectionPool("MySQLdb", **config)
        return cls(dbpool)

    def _insert_match(self, txn, item):
        sql = "INSERT INTO `lol_matches`(`b_match_id`,`b_match_name`," \
              "`game_id`,`game_name`,`game_type_id`,`game_mode`,`game_type_name`,`game_proc_id`,`game_proc_name`," \
              "`left_team_number`,`left_team_score`,`right_team_number`,`right_team_score`,`match_date`,`match_status`,`match_win`) " \
              "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        txn.execute(sql, [item['b_match_id'], item['b_match_name'], item['game_id'], item['game_name'],
                          item['game_type_id'], item['game_mode'], item['game_type_name'], item['game_proc_id'],
                          item['game_proc_name'], item['left_team_number'], item['left_team_score'],
                          item['right_team_number'], item['right_team_score'], item['match_date'], item['match_status'],
                          item['match_win']])

    def _insert_match_info(self, txn, item):
        sql = "INSERT INTO `lol_match_infos`(`s_match_id`,`s_match_name`,`b_match_id`,`b_match_name`," \
              "`game_id`,`game_name`,`game_proc_id`,`game_proc_name`,`match_num`,`area_id`," \
              "`battle_id`,`left_team_number`,`right_team_number`,`match_status`,`match_win`,`blue_team`) " \
              "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        txn.execute(sql, [item['s_match_id'], item['s_match_name'], item['b_match_id'], item['b_match_name'],
                          item['game_id'],
                          item['game_name'], item['game_proc_id'], item['game_proc_name'],
                          item['match_num'], item['area_id'], item['battle_id'], item['left_team_number'],
                          item['right_team_number'], item['match_status'], item['match_win'], item['blue_team'],
                          ])

    def _insert_match_battle(self, txn, item):
        sql = "INSERT INTO `lol_match_battles`(`battle_id`,`battle_date`,`battle_time`,`area_id`,`battle_data`,`s_updated`) " \
              "VALUES (%s,%s,%s,%s,%s,%s)"
        txn.execute(sql, [item['battle_id'], item['battle_date'], item['battle_time'], item['area_id'],
                          str(item['battle_data']),
                          item['s_updated']
                          ])

    def _insert_match_players(self, txn, item):
        sql = "INSERT INTO `lol_match_players`(`b_match_id`,`s_match_id`,`battle_id`," \
              "`area_id`,`team_id`,`member_id`,`account_id`,`game_name`,`place`,`champion_id`," \
              "`game_k`,`game_d`,`game_a`,`game_m`,`game_w`,`s_updated`,`i_mvp`,`i_dvp_num`) " \
              "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        txn.executemany(sql, item['match_members'])

    def process_item(self, item, spider):
        # 比赛
        if isinstance(item, LolMatchItem):
            self.dbpool.runInteraction(self._insert_match, item)
        # 比赛详情
        if isinstance(item, LolMatchInfoItem):
            self.dbpool.runInteraction(self._insert_match_info, item)
            self.dbpool.runInteraction(self._insert_match_battle, item)
            self.dbpool.runInteraction(self._insert_match_players, item)
