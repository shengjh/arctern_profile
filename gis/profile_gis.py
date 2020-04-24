# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pyarrow
import pandas
from osgeo import ogr
import arctern

rows = 10000000
func_name = 0
execute_times = 10

import time

output_path = "./output.txt"


def timmer(fun1):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        res = fun1(*args, **kwargs)
        stop_time = time.time()
        dur = stop_time - start_time
        with open(output_path, 'a') as f:
            f.write(fun1.__name__ + " " + str(dur) + "\n")
        return res

    return wrapper


def _trans(data):
    ret = pandas.Series([data])
    ret = arctern.ST_GeomFromText(ret)
    ret = [ret[0]] * rows
    ret = pandas.Series(ret)
    return ret


def _trans2(data):
    data_array = [data] * rows
    data = pandas.Series(data_array)
    data = arctern.ST_GeomFromText(data)
    return data


def gen_envelope_data():
    data = _trans('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)')
    return data


def gen_equals_data():
    data = _trans("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
    return data


def gen_geo_type_data():
    data = _trans("MULTIPOLYGON ( ((0 0, 0 4, 4 4, 4 0, 0 0)), ((0 0, 4 0, 4 1, 0 1, 0 0)) )")
    return data


def gen_issimple_data():
    data = _trans("MULTICURVE((0 0,5 5),CIRCULARSTRING (4 0,4 4,8 4))")
    return data


def gen_st_point_data():
    data1 = [1.3, 2.5] * rows
    data1 = pandas.Series(data1)
    data2 = [3.8, 4.9] * rows
    data2 = pandas.Series(data2)
    return data1, data2


def gen_st_distance_data():
    data1 = _trans("POINT(1 1)")
    data2 = _trans('POINT(5 2.1)')
    return data1, data2


@timmer
def test_ST_Envelope():
    rst = arctern.ST_Envelope(envelope_d)


@timmer
def test_ST_equals():
    rst = arctern.ST_Equals(equals_d1, equals_d2)


@timmer
def test_ST_geo_type():
    rst = arctern.ST_GeometryType(geo_type_d)


@timmer
def test_ST_issimple():
    rst = arctern.ST_IsSimple(issimple_d)


def parse_args(argv):
    import sys, getopt
    try:
        opts, args = getopt.getopt(argv, "rf:", ["unf function name"])
    except getopt.GetoptError:
        print('profile_geo.py -r <rows>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('profile_geo.py -r <rows> -f <udf function name>')
            sys.exit()
        elif opt in ("-r", "--rows"):
            global rows
            rows = int(arg)
        elif opt in ("-f", "--udf"):
            global func_name
            func_name = int(arg)


if __name__ == "__main__":
    funcs = [
        test_ST_equals,
        test_ST_geo_type,
        test_ST_issimple,
        test_ST_Envelope,
    ]
    envelope_d = gen_envelope_data()
    equals_d1 = equals_d2 = gen_equals_data()
    geo_type_d = gen_geo_type_data()
    issimple_d = gen_issimple_data()

    for f in funcs:
        for j in range(execute_times):
            f()
