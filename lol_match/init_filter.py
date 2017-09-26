# coding=utf-8
import MySQLdb

import sys

import redis

reload(sys)

sys.path.append('../../')


def run():
    m_conn = mysql_connect({
        'host': "127.0.0.1",
        "port": 3306,
        "user": "root",
        "passwd": "root",
        "db": "caikes",
        "charset": "utf8"
    })
    m_cur = m_conn.cursor()

    r_pool = redis_connect({
        'host': "192.168.10.10",
        'port': 6379,
        'db': 0
    })

    r_conn = redis.Redis(connection_pool=r_pool)

    lol_matches_query = "SELECT b_match_id FROM lol_matches"

    lol_match_info_query = "SELECT s_match_id FROM lol_match_infos"

    m_cur.execute(lol_matches_query)

    b_match_ids = m_cur.fetchall()

    b_match_count = 0

    for b_match_id in b_match_ids:
        result = r_conn.sadd('b_match_ids', b_match_id[0])
        if result:
            b_match_count += 1

    m_cur.execute(lol_match_info_query)

    s_match_ids = m_cur.fetchall()

    s_match_count = 0

    for s_match_id in s_match_ids:
        result = r_conn.sadd('s_match_ids', s_match_id[0])
        if result:
            s_match_count += 1

    print "共新增比赛:%d,新增比赛详情:%d" % (b_match_count, s_match_count)


def mysql_connect(conf):
    return MySQLdb.connect(host=conf['host'], port=conf['port'], user=conf['user'], passwd=conf['passwd'],
                           db=conf['db'], charset=conf['charset'])


def redis_connect(conf):
    return redis.ConnectionPool(host=conf['host'], port=conf['port'], db=conf['db'])


if __name__ == '__main__':
    run()
