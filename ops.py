#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/6/19 18:46
# @Author  : Yajun Yin
# @Note    :

from __future__ import print_function

import re
import time
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import six


class OperationException(Exception):
    pass


def camel2underline(camel_str):
    # 匹配正则，匹配小写字母和大写字母的分界位置
    p = re.compile(r'([a-z]|\d)([A-Z])')
    # 这里第二个参数使用了正则分组的后向引用
    sub = re.sub(p, r'\1_\2', camel_str).lower()
    return sub


class OpCollection(type):
    ops = defaultdict(dict)
    base_ops = []

    def __new__(mcs, name, bases, attrs):
        base_ops = mcs.base_ops
        if name in base_ops:
            return type.__new__(mcs, name, bases, attrs)
        cls = type.__new__(mcs, name, bases, attrs)
        for base in base_ops:
            if name.endswith(base) and not name.startswith("_"):
                name = camel2underline(name[:-len(base)])
                mcs.ops[base][name] = cls
        return cls


def base_op(cls):
    """a decorator that register base op into OpCollection.base_ops"""
    OpCollection.base_ops.append(cls.__name__)
    return cls


def show_ops():
    for base_op, ops in OpCollection.ops.items():
        print(base_op)
        for op_name, op in ops.items():
            print("\t", op_name, ":", op.__name__)


##########################
#    agg op
##########################

@base_op
class AggOp(object, six.with_metaclass(OpCollection)):
    def check_hist(self, hist):
        # hist expects to be a list, whose element is like (value, timestamp) under the same id.
        if not isinstance(hist, list):
            raise TypeError("%s's argument hist expects type is list, got %s" % (self.__class__.__name__, type(hist)))

    def transform(self, hist=None):
        raise NotImplementedError


class SumAggOp(AggOp):
    def transform(self, hist=None):
        self.check_hist(hist)
        _values = [float(i[0]) for i in hist]
        _sum_value = sum(_values)
        _last_time = hist[-1][1]
        return _sum_value, _last_time


class MaxAggOp(AggOp):
    def transform(self, hist=None):
        self.check_hist(hist)
        _values = [float(i[0]) for i in hist]
        idx = np.argmax(_values)
        _value, _time = hist[idx][0], hist[idx][1]
        return _value, _time


class FirstAggOp(AggOp):
    def transform(self, hist=None):
        self.check_hist(hist)
        if len(hist) == 0:
            return None, None
        _value, _time = hist[0][0], hist[0][1]
        return _value, _time


class LastAggOp(AggOp):
    def transform(self, hist=None):
        self.check_hist(hist)
        if len(hist) == 0:
            return None, None
        _value, _time = hist[-1][0], hist[-1][1]
        return _value, _time


class DefaultAggOp(AggOp):
    def transform(self, hist=None):
        op = MaxAggOp()
        return op.transform(hist)


##########################
#    stat op
##########################

@base_op
class StatOp(object, six.with_metaclass(OpCollection)):
    def check_groupings(self, dimension_grouping):
        # dimension_grouping: <dimension, (value, timestamp)>, dimension may be not distinct
        if not isinstance(dimension_grouping, dict):
            raise TypeError("%s's argument dimension_grouping expects type "
                            "is dict, got %s" % (self.__class__.__name__, type(dimension_grouping)))

    def check_end_date(self, end_date):
        if not isinstance(end_date, six.text_type):
            raise TypeError(
                "%s's argument end_date expects type is six.text_type, got %s" % (
                    self.__class__.__name__, type(end_date)))
        try:
            datetime.strptime(end_date, "%Y%m%d")
        except ValueError:
            raise ValueError("{0}'s argument end_date does not match format '%Y%m%d'".format(self.__class__.__name__))

    def transform(self, dimension_grouping=None, end_date=None):
        raise NotImplementedError


class HistStatOp(StatOp):
    def transform(self, dimension_grouping=None, end_date=None):
        self.check_groupings(dimension_grouping)
        _sorted = sorted(dimension_grouping.items(), key=lambda p: p[1][1])  # sort depends on timestamp
        res = ["%s:%s" % (six.text_type(k), six.text_type(v[0])) for k, v in _sorted]
        return ",".join(res)


class IdentityStatOp(StatOp):
    def transform(self, dimension_grouping=None, end_date=None):
        self.check_groupings(dimension_grouping)
        return dimension_grouping


class SumPeriodStatOp(StatOp):
    def transform(self, dimension_grouping=None, period=None, end_date=None):
        self.check_groupings(dimension_grouping)
        self.check_end_date(end_date)
        if not isinstance(period, int):
            raise ValueError("period type expects int.")
        end_ts = date_to_timestamp(end_date)
        start_ts = get_timestamp(period, end_ts)
        _filter = filter(lambda p: start_ts <= p[1][1] <= end_ts, dimension_grouping.items())
        res = sum([float(i[1][0]) for i in _filter])
        return res


class Sum30StatOp(StatOp):
    def transform(self, dimension_grouping=None, end_date=None):
        op = SumPeriodStatOp()
        return op.transform(dimension_grouping=dimension_grouping, period=30, end_date=end_date)


class Sum14StatOp(StatOp):
    def transform(self, dimension_grouping=None, end_date=None):
        op = SumPeriodStatOp()
        return op.transform(dimension_grouping=dimension_grouping, period=14, end_date=end_date)


class Sum5StatOp(StatOp):
    def transform(self, dimension_grouping=None, end_date=None):
        op = SumPeriodStatOp()
        return op.transform(dimension_grouping=dimension_grouping, period=5, end_date=end_date)


class DefaultStatOp(StatOp):
    def transform(self, dimension_grouping=None, end_date=None):
        op = IdentityStatOp()
        return op.transform(dimension_grouping=dimension_grouping, end_date=end_date)


##########################
#    trans op
##########################

@base_op
class TransOp(object, six.with_metaclass(OpCollection)):
    def check_value(self, value=None, allowed=(six.text_type, int, float, list)):
        if not isinstance(value, allowed):
            raise TypeError(
                "%s's argument value expects type is (six.text_type, int, float, list), got %s" % (
                    self.__class__.__name__, type(value)))

    def transform(self, value=None, **kwargs):
        raise NotImplementedError


class IdentityTransOp(TransOp):
    def transform(self, value=None, **kwargs):
        self.check_value(value)
        return value


class DefaultTransOp(TransOp):
    def transform(self, value=None, **kwargs):
        obj = IdentityTransOp()
        return obj.transform(value)


class StrContainTransOp(TransOp):
    def transform(self, value=None, target=None, **kwargs):
        self.check_value(value=value, allowed=(six.text_type,))
        if target in value:
            return 1
        return 0


class _EvalTransOp(TransOp):
    def transform(self, value=None, **kwargs):
        self.check_value(value=value, allowed=(six.text_type,))
        return eval(value)


class ArrayLenTransOp(TransOp):
    def transform(self, value=None, **kwargs):
        self.check_value(value=value, allowed=(six.text_type, list))
        v = value
        if isinstance(value, six.text_type):
            v = _EvalTransOp().transform(value)
        elif isinstance(value, list):
            v = value
        return len(v)


class BucketTransOp(TransOp):
    def transform(self, value=None, splitter=None, **kwargs):
        self.check_value(value=value, allowed=(six.text_type, int, float, list))


class ScalerMinMaxTransOp(TransOp):
    def transform(self, value=None, _min=None, _max=None, **kwargs):
        self.check_value(value=value, allowed=(int, float))
        self.check_value(value=_min, allowed=(int, float))
        self.check_value(value=_max, allowed=(int, float))
        if _max <= _min:
            raise ValueError("_min <= _max")
        return float(value - _min) / float(_max - _min)


class ScalerZscoreTransOp(TransOp):
    def transform(self, value=None, _mean=None, _std=None, **kwargs):
        self.check_value(value=value, allowed=(int, float))
        self.check_value(value=_mean, allowed=(int, float))
        self.check_value(value=_std, allowed=(int, float))
        if _std <= 0:
            raise ValueError("_std <= 0")
        return float(value - _mean) / float(_std)


class NormalizationNormTransOp(TransOp):
    def transform(self, value=None, **kwargs):
        self.check_value(value=value, allowed=(list,))
        norm = np.linalg.norm(value)
        return [i / norm for i in value]


class NormalizationAccountTransOp(TransOp):
    def transform(self, value=None, **kwargs):
        self.check_value(value=value, allowed=(list,))
        _sum = sum(value)
        return [i / _sum for i in value]


class SaveDivideTransOp(TransOp):
    def transform(self, value=None, **kwargs):
        self.check_value(value=value, allowed=(list,))
        assert len(value) == 2
        try:
            a, b = float(value[0]), float(value[1])
            if b == 0:
                return 0.0
            return a / b
        except Exception as e:
            raise OperationException("SaveDivideTransOp", value, kwargs, e)


##########################
#    utils
##########################
FORMAT1 = '%Y%m%d'
FORMAT2 = '%Y-%m-%d'


def get_timestamp(period, timestamp):
    start = (datetime.fromtimestamp(timestamp) - timedelta(period))
    ts = int(time.mktime(start.timetuple()))
    return ts


# %Y%m%d -> timestamp
def date_to_timestamp(day_str):
    d = datetime.strptime(day_str, '%Y%m%d')
    ts = int(time.mktime(d.timetuple()))
    return ts
