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

from .ops import OpCollection
from .util import date2timestamp

AllowedBaseOpTypes = ["TransOp", "AggOp", "StatOp"]


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

    @abc.abstractmethod
    def keys(self, obj):
        pass

    @abc.abstractmethod
    def to_dict(self, obj):
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

    def keys(self, obj):
        return obj.keys()

    def to_dict(self, obj):
        return obj


class FeatureGenerator(ConfigBackend):
    # --------------------------
    #  init
    # --------------------------
    def __init__(self, backend="json", show=True):
        self.config = None
        self.show = show
        self._init_backend(backend)
        self._init_ops()

    def _init_ops(self):
        self.ops = OpCollection.ops
        if self.show:
            self._show_ops()

    def _init_backend(self, backend):
        if backend == "json":
            self.backend = JsonConfig()
        else:
            raise ValueError("%s not support" % backend)
        if self.show:
            print("Config Parser Backend is [%s]\n" % backend)
            self._show_config()

    def _update_config(self, file):
        self.config = self.read_config(file)
        self._show_config()

    def _show_ops(self):
        print("Supported Ops:\n")
        self._recursive_print(self.ops)
        print("\n")

    def _show_config(self):
        if self.config:
            print("Current Config: \n")
            self._recursive_print(self.config)
            print("\n")

    # --------------------------
    #  config
    # --------------------------
    def read_config(self, file):
        return self.backend.read_config(file)

    def get(self, obj, item):
        return self.backend.get(obj, item)

    def get_with_default(self, obj, item, default):
        return self.backend.get_with_default(obj, item, default)

    def has(self, obj, item):
        return self.backend.has(obj, item)

    def keys(self, obj):
        return self.backend.keys(obj)

    def to_dict(self, obj):
        return self.backend.to_dict(obj)

    # --------------------------
    #  basic
    # --------------------------
    def _default_parse(self, obj, main_item="Column", base_op_types=[]):
        main = self.get(obj, main_item)
        name = self.get_with_default(obj, "Name", default=main)
        assert isinstance(name, six.text_type), (type(name), main, name)
        ops = []
        for bt in base_op_types:
            if bt not in AllowedBaseOpTypes:
                raise ValueError("base op type error")
            ops.append(self._parse_op(obj, bt))
        return name, main, ops

    def _parse_op(self, obj_config, base_op_type):
        """
        :param obj_config: specific config
        :param base_op_type: "Stat", "Trans" or "Agg"
        :return: op_instance, args
        """
        op, args = None, None
        op_config = self.get_with_default(obj_config, base_op_type, six.text_type("default"))
        if isinstance(op_config, six.text_type):
            op_name = op_config
        else:  # op_config is like {"Op": xx, Args:xxx}
            op_name = self.get(op_config, "Op")
            args = self.get(op_config, "Args")
            assert isinstance(args, dict)
        op = self.get_op(base_op_type, op_name)()
        return op, args

    @staticmethod
    def _execute_op(op_spec, value, **extra_args):
        op, args = op_spec
        if isinstance(args, dict):
            args.update(extra_args)
        else:
            args = extra_args
        return op.transform(value, **args)

    def _recursive_print(self, obj, indent=""):
        try:
            if isinstance(obj, list):
                for ele in obj:
                    print(indent + "-" * 20)
                    self._recursive_print(ele, indent)
                    print(indent + "-" * 20)
            else:
                for key in self.keys(obj):
                    print(indent, key)
                    self._recursive_print(self.get(obj, key), "    " + indent)
        except AttributeError:
            if hasattr(obj, "__name__"):
                print(indent, obj.__name__)
            else:
                print(indent, obj)

    # --------------------------
    #  direct feature
    # --------------------------
    def transform_direct(self, file, df):
        self._update_config(file)
        rdd = df.rdd.map(self._transform1)
        df = rdd.toDF()
        return df

    def _transform1(self, struct):
        tmp = OrderedDict()
        features = self.get(self.config, "PrimaryFeatures")
        assert isinstance(features, list)
        for f in features:
            name, col, trans_spec = self._parses_direct_feature(f)
            trans_op, args = trans_spec
            assert col in struct
            tmp[name] = trans_op.transform(hist=struct[col], **args)
        row = Row(*tmp.keys())
        res = row(*tmp.values())
        return res

    # --------------------------
    #  aggregated feature
    # --------------------------
    def _first_aggregate(self, df):
        """first group"""
        _, cols = self._parse_group()
        g = df.rdd.groupBy(lambda row: self._make_group(row=row, cols=cols))
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
            raise ValueError("invalid end_date", end_ts, timestamp)
        _decay = exponential_decay(ts_delta / float(24 * 3600), finish)
        return value * _decay, timestamp

    def _aggregate_mapping(self, mapping, op_spec):
        for key, value in mapping.items():
            mapping[key] = self._execute_op(op_spec, value)

    def _second_aggregate(self, grouped_items, dim_conf):
        """
        aggregate grouped_items according to dimensions
        """
        dim_value_conf = self.get(dim_conf, "DimValue")
        prim_feats_conf = self.get(dim_conf, "PrimaryFeatures")
        comp_feats_conf = self.get(dim_conf, "CompositeFeatures")
        decay_conf = self.get(self.config, "Decay")
        decay_col, decay_end, decay_finish = self._parse_decay(decay_conf)
        # generating features
        features = OrderedDict()
        # primary_features(agg, stat)
        for prim_feat in prim_feats_conf:
            _mapping = defaultdict(list)
            name, col, stat_spec, agg_spec = self._parse_aggregated_feature(prim_feat)
            for item in grouped_items:
                dim_value = self._retrieve_dimension(item, dim_value_conf)
                timestamp = self._retrieve_timestamp(item, decay_col)
                value = item[col]
                # time decay
                try:
                    v, t = self._time_decay(value, timestamp, decay_end, decay_finish)
                    _mapping[dim_value].append((v, t))
                except ValueError:  # timestamp >= decay end
                    continue
            # aggregate values under the same dimension value
            self._aggregate_mapping(_mapping, agg_spec)
            # execute stat op on aggregated values
            features[name] = self._execute_op(stat_spec, _mapping, end_date=decay_end)
        # composite features(trans)
        for comp_feat in comp_feats_conf:
            name, prim_feat, trans_spec = self._parse_composite_feature(comp_feat)
            if name in features:
                # todo: check first
                raise ValueError("Duplicated name", name)
            if isinstance(prim_feat, list):
                value = [features[i] for i in prim_feat]
            else:
                assert isinstance(prim_feat, six.text_type)
                value = features[prim_feat]
            features[name] = self._execute_op(trans_spec, value)
        return features

    def _transform2(self, group_items):
        group_key, grouped_items = group_items[0], group_items[1]
        group_name, _ = self._parse_group()
        feats = OrderedDict()
        feats[group_name] = group_key
        for dimension_conf in self.get(self.config, "Dimensions"):
            new_feats = self._second_aggregate(grouped_items, dimension_conf)
            feats.update(new_feats)
        row = Row(*feats.keys())
        res = row(*feats.values())
        return res

    def transform_aggregated(self, file, df):
        self._update_config(file)
        grouped_rdd = self._first_aggregate(df)
        rdd = grouped_rdd.map(self._transform2)
        df = rdd.toDF()
        return df

    # --------------------------
    #  util
    # --------------------------
    def _parses_direct_feature(self, feature_config):
        name, col, ops = self._default_parse(feature_config, base_op_types=["TransOp"])
        assert len(ops) == 1
        trans_op, args = ops[0]
        return name, col, (trans_op, args)

    def _parse_aggregated_feature(self, primary_feature_config):
        name, col, ops = self._default_parse(primary_feature_config, base_op_types=["StatOp", "AggOp"])
        assert len(ops) == 2
        stat_op, args1 = ops[0]
        agg_op, args2 = ops[1]
        return name, col, (stat_op, args1), (agg_op, args2)

    def _parse_composite_feature(self, composite_feature_config):
        name, primary_feature, ops = self._default_parse(composite_feature_config,
                                                         main_item="PrimaryFeature",
                                                         base_op_types=["TransOp"])
        assert len(ops) == 1
        trans_op, args = ops[0]
        return name, primary_feature, (trans_op, args)

    def _parse_group(self):
        group = self.get(self.config, "Group")
        name, col, _ = self._default_parse(group)
        return name, col

    def get_op(self, base_op, op_name):
        op_collection = self.get(self.ops, base_op)
        op = self.get(op_collection, op_name)
        return op

    def _retrieve_dimension(self, item, dim_value):
        if isinstance(dim_value, list):
            res = []
            for d in dim_value:
                col, trans_spec = self._parse_dim_value(d)
                new_dim_value = self._execute_op(trans_spec, item[col])
                res.append(six.text_type(new_dim_value))
            return "*".join(res)
        else:  # single dim value
            col, trans_spec = self._parse_dim_value(dim_value)
            new_dim_value = self._execute_op(trans_spec, item[col])
            return new_dim_value

    def _parse_dim_value(self, dim_value_config):
        _, col, trans_spec = self._parses_direct_feature(dim_value_config)
        return col, trans_spec

    def _parse_decay(self, decay_config):
        col = self.get(decay_config, "Column")
        yesterday = datetime.strftime(datetime.today() - timedelta(1), "%Y%m%d")
        end_date = self.get_with_default(decay_config, "EndDate", default=yesterday)
        finish = self.get_with_default(decay_config, "Finish", default=1.0)
        try:
            datetime.strptime(end_date, "%Y%m%d")
        except ValueError as e:
            raise ValueError("end date parse failed", e)
        assert isinstance(finish, float)
        return col, end_date, finish

    def _retrieve_timestamp(self, item, col):
        return int(self.get(item, col))


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
