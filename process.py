#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/6/22 10:30
# @Author  : Yajun Yin
# @Note    :

from __future__ import print_function

import abc
import json
import numpy as np
import six
from collections import defaultdict, OrderedDict
from datetime import datetime, timedelta
from pyspark.sql import Row

from .ops import OpCollection, show_ops
from .util import date2timestamp


class ConfigBackend(object, six.with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def read_config(self, file):
        pass

    @abc.abstractmethod
    def get(self, obj, item):
        pass

    @abc.abstractmethod
    def get_with_default(self, obj, item, default):
        pass

    @abc.abstractmethod
    def has(self, obj, item):
        pass


class JsonConfig(ConfigBackend):
    def read_config(self, file):
        with open(file) as f:
            conf = json.load(f)
            return conf

    def get(self, obj, item):
        try:
            return obj[item]
        except KeyError:
            raise KeyError("%s not exist" % item)

    def get_with_default(self, obj, item, default):
        return obj.get(item, default)

    def has(self, obj, item):
        return item in obj


class TomlConfig(ConfigBackend):
    pass


class YamlConfig(ConfigBackend):
    pass


class FeatureGenerator(object):
    def __init__(self, backend="json"):
        self._init_ops()
        self._init_backend(backend)
        self.config = None

    def _init_ops(self):
        self.ops = OpCollection.ops
        print("[Init Ops] Support Ops as follow:")
        show_ops()

    def _init_backend(self, backend):
        if backend == "json":
            self.backend = JsonConfig()
        # elif backend == "toml":
        #     self.backend = TomlConfig()
        # elif backend == "yaml":
        #     self.backend = YamlConfig()
        else:
            raise ValueError("%s not support" % backend)
        print("[Init Config] parser backend is %s" % backend)

    def _update_config(self, file):
        self.config = self.backend.read_config(file)

    def _get(self, obj, item):
        return self.backend.get(obj, item)

    def _get_with_default(self, obj, item, default):
        return self.backend.get_with_default(obj, item, default)

    def _has(self, obj, item):
        return self.backend.has(obj, item)

    # --------------------------
    #  first feature
    # --------------------------
    def transform_first(self, file, df):
        self._update_config(file)
        rdd = df.rdd.map(self._transform_first)
        df = rdd.toDF()
        return df

    def _transform_first(self, struct):
        tmp = OrderedDict()
        features = self._get_features()
        assert isinstance(features, list)
        for f in features:
            col, name, op = self._parse_first_feature(f)
            assert col in struct
            trans_op = self._get_trans_op(op)
            tmp[name] = trans_op.transform(struct[col])
        row = Row(*tmp.keys())
        res = row(*tmp.values())
        return res

    # --------------------------
    #  second feature
    # --------------------------
    def _first_grouped(self, df):
        """first group"""
        _, cols = self._parse_group()
        g = df.rdd.groupBy(lambda p: self._make_group(row=p, cols=cols))
        return g

    @staticmethod
    def _make_group(row, cols):
        if isinstance(cols, list):
            for col in cols:
                assert col in row.__fields__, col
            return tuple(row[col] for col in cols)
        else:
            assert cols in row.__fields__, cols
            return row[cols]

    @staticmethod
    def _time_decay(value, timestamp, end_date, finish):
        end_ts = date2timestamp(end_date)
        ts_delta = end_ts - timestamp
        if ts_delta < 0:
            raise ValueError("invalid end_date")
        _decay = exponential_decay(ts_delta / float(24 * 3600), finish)
        return value * _decay, timestamp

    def _aggregate_value(self, mapping, op_name):
        op = self._get_agg_op(op_name)
        for dim, hist in mapping.items():
            mapping[dim] = op.transform(hist=hist)

    def _stat_value(self, dimension_mapping, op_name, end_date):
        op = self._get_stat_op(op_name)
        ret = op.transform(dimension_grouping=dimension_mapping, end_date=end_date)
        return ret

    def _second_grouped(self, grouped_items, feature, decay):
        """
        second group
        group_items is the product of first group
        second group occurs in group_items
        """
        dims, entities = self._parse_feature(feature)
        decay_col, decay_end, decay_finish = self._parse_decay(decay)
        features = OrderedDict()
        for entity in entities:
            dimension_mapping = defaultdict(list)
            col, name, stat_op_name, agg_op_name = self._parse_entity(entity)
            for item in grouped_items:
                dim_value = self._retrieve_dimension(item, dims)
                timestamp = self._retrieve_timestamp(item, decay_col)
                value = item[col]
                # time decay
                v, t = self._time_decay(value, timestamp, decay_end, decay_finish)
                dimension_mapping[dim_value].append((v, t))
            # aggregate values under the same dimension value
            self._aggregate_value(dimension_mapping, agg_op_name)
            # save statistical value in features
            features[name] = self._stat_value(dimension_mapping, stat_op_name, decay_end)
        return features

    def _transform_second(self, group_items):
        group_key, grouped_items = group_items[0], group_items[1]
        group_name, _ = self._parse_group()
        features = OrderedDict()
        features[group_name] = group_key
        decay = self._get_decay()
        for fea in self._get_features():
            new_features = self._second_grouped(grouped_items, fea, decay)
            features.update(new_features)
        row = Row(*features.keys())
        res = row(*features.values())
        return res

    def transform_second(self, file, df):
        self._update_config(file)
        grouped_rdd = self._first_grouped(df)
        rdd = grouped_rdd.map(self._transform_second)
        df = rdd.toDF()
        return df

    # --------------------------
    #  util
    # --------------------------

    def _get_decay(self):
        return self._get(self.config, "Decay")

    def _get_features(self):
        return self._get(self.config, "Features")

    def _parse_group(self):
        group = self._get(self.config, "Group")
        col = self._get(group, "Column")
        if isinstance(col, list):
            name = self._get(group, "Name")
        else:  # col should be str
            assert isinstance(col, six.text_type), type(col)
            name = col
        return name, col

    def _parse_first_feature(self, feature):
        col = self._get(feature, "Column")
        name = self._get(feature, "Name")
        op = self._get_with_default(feature, "Trans", default="default")
        return col, name, op

    def _get_trans_op(self, op_name):
        return self._get_op(self.ops, "TransOp", op_name)()

    def _get_stat_op(self, op_name):
        return self._get_op(self.ops, "StatOp", op_name)()

    def _get_agg_op(self, op_name):
        return self._get_op(self.ops, "AggOp", op_name)()

    def _get_op(self, op_collections, base_op, op_name):
        op_collection = self._get(op_collections, base_op)
        op = self._get(op_collection, op_name)
        return op

    def _retrieve_dimension(self, item, dims):
        if isinstance(dims, list):
            res = []
            for d in dims:
                col, trans_op_name = self._parse_dimension(d)
                op = self._get_trans_op(trans_op_name)
                new_dim = op.transform(item[col])
                res.append(six.text_type(new_dim))
            return "*".join(res)
        else:  # dims is only one dim
            col, trans_op_name = self._parse_dimension(dims)
            op = self._get_trans_op(trans_op_name)
            return op.transform(item[col])

    def _parse_dimension(self, dimension):
        col = self._get(dimension, "Column")
        trans_op_name = self._get_with_default(dimension, "Trans", default="default")
        return col, trans_op_name

    def _parse_decay(self, decay):
        col = self._get(decay, "Column")
        if self._has(decay, "EndDate"):
            end_date = self._get(decay, "EndDate")
        else:
            yesterday = datetime.strftime(datetime.today() - timedelta(1), "%Y%m%d")
            end_date = yesterday
        finish = self._get(decay, "Finish")
        return col, end_date, finish

    def _retrieve_timestamp(self, item, col):
        return int(self._get(item, col))

    def _parse_feature(self, feature):
        dims = self._get(feature, "Dimensions")
        entities = self._get(feature, "Entities")
        return dims, entities

    def _parse_entity(self, entity):
        col = self._get(entity, "Column")
        name = self._get(entity, "Name")
        stat_op_name = self._get_with_default(entity, "Stat", default="default")
        agg_op_name = self._get_with_default(entity, "Agg", default="default")
        return col, name, stat_op_name, agg_op_name


def _exponential_decay(t, init=1.0, m=30, finish=0.5):
    """Newton's law of cooling"""
    alpha = np.log(init / finish) / m
    l = - np.log(init) / alpha
    decay = np.exp(-alpha * (t + l))
    return decay


def exponential_decay(t, finish):
    """fix init=1.0 and m=30"""
    if finish == 1.0:
        return finish
    return _exponential_decay(t, finish=finish)
