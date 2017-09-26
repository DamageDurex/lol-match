# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class LolMatchItem(scrapy.Item):
    # 比赛id
    b_match_id = scrapy.Field()

    # 比赛名称
    b_match_name = scrapy.Field()

    # game id
    game_id = scrapy.Field()

    # 游戏名称
    game_name = scrapy.Field()

    # game type id
    game_type_id = scrapy.Field()

    # 比赛模式 (0：默认值 1：未开始 2：进行中 3：已结束)
    game_mode = scrapy.Field()

    # 游戏类型名称
    game_type_name = scrapy.Field()

    # 比赛进行id(和周对应)
    game_proc_id = scrapy.Field()

    # 比赛进行名称(第x周)
    game_proc_name = scrapy.Field()

    # 战队1编号
    left_team_number = scrapy.Field()

    # 战队1名称
    # left_team_name = scrapy.Field()

    # 战队1得分
    left_team_score = scrapy.Field()

    # 战队2编号
    right_team_number = scrapy.Field()

    # 战队1名称
    # right_team_name = scrapy.Field()

    # 战队2得分
    right_team_score = scrapy.Field()

    # 比赛时间
    match_date = scrapy.Field()

    # 比赛状态
    match_status = scrapy.Field()

    # 获胜方
    match_win = scrapy.Field()


class LolMatchInfoItem(scrapy.Field):

    # -------------比赛信息------------- #
    # 比赛详情id
    s_match_id = scrapy.Field()

    # 比赛进行场次名称(第X场)
    s_match_name = scrapy.Field()

    # 比赛名称
    b_match_id = scrapy.Field()

    # 交战名称
    b_match_name = scrapy.Field()

    # 比赛id
    game_id = scrapy.Field()

    # 比赛名称
    game_name = scrapy.Field()

    # 比赛进行id(和周对应)
    game_proc_id = scrapy.Field()

    # 比赛进行名称(第x周)
    game_proc_name = scrapy.Field()

    # 比赛场次
    match_num = scrapy.Field()

    # 赛区id
    area_id = scrapy.Field()

    # battle id
    battle_id = scrapy.Field()

    # team_a 编号
    left_team_number = scrapy.Field()

    # team b 编号
    right_team_number = scrapy.Field()

    # 比赛状态 1未开始 2进行中 3已结束
    match_status = scrapy.Field()

    # 获胜方
    match_win = scrapy.Field()

    # 蓝色方 编号
    blue_team = scrapy.Field()

    # -------------比赛battle信息------------- #
    # 交战日期
    battle_date = scrapy.Field()

    # 交战时间
    battle_time = scrapy.Field()

    # 交战数据
    battle_data = scrapy.Field()

    # 更新时间
    s_updated = scrapy.Field()

    # -------------比赛member信息------------- #
    match_members = scrapy.Field()
