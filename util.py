#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/6/22 15:48
# @Author  : Yajun Yin
# @Note    :


import json
import time
from datetime import datetime, timedelta


def read_config(config_file):
    """
    :param config_file: absolute path
    :return:
    """
    with open(config_file) as f:
        conf = json.load(f)
        return conf


def get_day(period_num, target_day, form='%Y%m%d'):
    day = (datetime.strptime(target_day, '%Y%m%d') - timedelta(period_num)).strftime(form)
    return day


def get_period(period_num, target_day):
    """
    获取一段时间的日期字符串组成的列表
    :param period_num: 区间长度（天）
    :param target_day: 最后一天的日期字符串，形式20190903
    :return: ['20190901', '20190902'， '20190903']
    """
    days = []
    for i in range(period_num):
        day = (datetime.strptime(target_day, '%Y%m%d') - timedelta(i)).strftime('%Y%m%d')
        days.append(day)
    return days


def date2timestamp(date):
    dt = datetime.strptime(date, "%Y%m%d")
    ts = int(time.mktime(dt.timetuple()))
    return ts
