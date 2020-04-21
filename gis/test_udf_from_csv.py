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

import os
import sys
import time
import pyarrow as pa

from arctern_pyspark import register_funcs, union_aggr, envelope_aggr
from pyspark.sql import SparkSession


data_path = ""
output_path = ""
test_name = []
hdfs_url = ""
to_hdfs = False
fs = None
show_df = False
exec_times = 6

maps = {

        'st_area': (
            'single_polygon',  #file_name
            'geos string', #schema
            "select ST_Area(ST_GeomFromText(geos)) from %s", #sql
            ),

        'st_astext': (
            'single_polygon',
            'geos string',
            # "select ST_AsText(ST_PolygonFromText(geos)) from %s",
            "select ST_PolygonFromText(geos) from %s",
            ),

        'st_envelope_aggr': (
            'single_polygon',
            'geos string',
            "select ST_GeomFromText(geos) as geos from %s",
            ),

        'st_envelope': (
            'single_polygon',
            'geos string',
            "select ST_Envelope(ST_GeomFromText(geos)) from %s",
            # "select ST_AsText(ST_Envelope(ST_GeomFromText(geos))) from %s",
            ),

        'st_geomfromwkt': (
            'single_polygon',
            'geos string',
            "select ST_GeomFromWKT(geos) from %s",
            # "select ST_AsText(ST_GeomFromWKT(geos)) from %s",
            ),

        'st_union_aggr': (
            'single_polygon',
            'geos string',
            "select ST_GeomFromText(geos) as geos from %s",
            ),

        'st_issimple': (
            'single_polygon',
            'geos string',
            "select ST_IsSimple(ST_GeomFromText(geos)) from %s",
            ),

        'st_polygonfromtext': (
            'single_polygon',
            'geos string',
            "select ST_PolygonFromText(geos) from %s",
            # "select ST_AsText(ST_PolygonFromText(geos)) from %s",
            ),

        'st_pointfromtext': (
            'single_point',
            'geos string',
            "select ST_PointFromText(geos) from %s",
            # "select ST_AsText(ST_PointFromText(geos)) from %s",
            ),

        'st_transform': (
            'single_point',
            'geos string',
            "select ST_Transform(ST_GeomFromText(geos), 'epsg:4326', 'epsg:3857') from %s",
            # "select ST_AsText(ST_Transform(ST_GeomFromText(geos), 'epsg:4326', 'epsg:3857')) from %s",
            ),

        'st_buffer': (
            'single_point',
            'geos string',
            "select ST_Buffer(ST_GeomFromText(geos), 1.2) from %s",
            # "select ST_AsText(ST_Buffer(ST_GeomFromText(geos), 1.2)) from %s",
            ),

        'st_length': (
            'single_linestring',
            'geos string',
            "select ST_Length(ST_GeomFromText(geos)) from %s",
            ),

        'st_linestringfromtext': (
            'single_lingstring',
            'geos string',
            "select ST_LineStringFromText(geos) from %s",
            # "select ST_AsText(ST_LineStringFromText(geos)) from %s",
            ),

        'st_isvalid': (
            'single_col',
            'geos string',
            "select ST_IsValid(ST_GeomFromText(geos)) from %s",
            ),

        'st_npoints': (
            'single_col',
            'geos string',
            "select ST_NPoints(ST_GeomFromText(geos)) from %s",
        ),

        'st_centroid': (
            'single_col',
            'geos string',
            "select ST_Centroid(ST_GeomFromText(geos)) from %s",
            # "select ST_AsText(ST_Centroid(ST_GeomFromText(geos))) from %s",
        ),

        'st_convexhull': (
            'single_col',
            'geos string',
            "select ST_convexhull(ST_GeomFromText(geos)) from %s",
            # "select ST_AsText(ST_convexhull(ST_GeomFromText(geos))) from %s",
        ),

        'st_geometry_type': (
            'single_col',
            'geos string',
            "select ST_GeometryType(ST_GeomFromText(geos)) from %s",
        ),

        'st_simplify_preserve_topology': (
            'single_col',
            'geos string',
            "select ST_SimplifyPreserveTopology(ST_GeomFromText(geos), 10) from %s",
            # "select ST_AsText(ST_SimplifyPreserveTopology(ST_GeomFromText(geos), 10)) from %s",
        ),

        'st_make_valid': (
            'single_col',
            'geos string',
            "select ST_MakeValid(ST_GeomFromText(geos)) from %s",
            # "select ST_AsText(ST_MakeValid(ST_GeomFromText(geos))) from %s",
        ),

        'st_intersection': (
            'double_col',
            "left string, right string",
            "select ST_Intersection(ST_GeomFromText(left), ST_GeomFromText(right)) from %s",
            # "select ST_AsText(ST_Intersection(ST_GeomFromText(left), ST_GeomFromText(right))) from %s",
        ),


        'st_intersects': (
            'double_col',
            "left string, right string",
            "select ST_Intersects(ST_GeomFromText(left), ST_GeomFromText(right)) from %s",
        ),

        'st_overlaps': (
            'double_col',
            "left string, right string",
            "select ST_Overlaps(ST_GeomFromText(left), ST_GeomFromText(right)) from %s",
        ),

        'st_contains': (
            'double_col',
            "left string, right string",
            "select ST_Contains(ST_GeomFromText(left), ST_GeomFromText(right)) from %s",
        ),

        'st_equals': (
            'double_col',
            "left string, right string",
            "select ST_Equals(ST_GeomFromText(left), ST_GeomFromText(right)) from %s",
        ),

        'st_crosses': (
            'double_col',
            "left string, right string",
            "select ST_Crosses(ST_GeomFromText(left), ST_GeomFromText(right)) from %s",
        ),

        'st_touches': (
            'double_col',
            "left string, right string",
            "select ST_Touches(ST_GeomFromText(left), ST_GeomFromText(right)) from %s",
        ),

        'st_hausdorffdistance': (
            'double_col',
            "left string, right string",
            "select ST_HausdorffDistance(ST_GeomFromText(left),ST_GeomFromText(right)) from %s",
        ),

        'st_distance': (
            'st_distance',
            "left string, right string",
            "select ST_Distance(ST_GeomFromText(left), ST_GeomFromText(right)) from %s",
        ),

        'st_within': (
            'st_within',
            "left string, right string",
            "select ST_Within(ST_GeomFromText(left), ST_GeomFromText(right)) from %s",
        ),

        'st_curvetoline': (
            'st_curvetoline',
            'geos string',
            "select ST_CurveToLine(ST_GeomFromText(geos)) from %s",
            # "select ST_AsText(ST_CurveToLine(ST_GeomFromText(geos))) from %s",
        ),

        'st_point': (
            'st_point',
            'x double, y double',
            # "select ST_AsText(ST_Point(x, y)) from %s",
            "select ST_Point(x, y) from %s",
        ),

        'st_polygon_from_envelope': (
            'st_polygon_from_envelope',
            "min_x double, min_y double, max_x double, max_y double",
            "select ST_PolygonFromEnvelope(min_x, min_y, max_x, max_y) from %s",
            # "select ST_AsText(ST_PolygonFromEnvelope(min_x, min_y, max_x, max_y)) from %s",
        ),

        'st_geomfromgeojson': (
            'st_geomfromgeojson',
            'geos string',
            "select ST_GeomFromGeoJSON(geos) from %s",
            # "select ST_AsText(ST_GeomFromGeoJSON(geos)) from %s",
        ),

}

def get_file_path(file_name):
    print("file_name", file_name)
    file_path = os.path.join(data_path, file_name + ".csv")
    if not file_name or not fs.exists(file_path):
        if file_name:
            print("failed to load file %s"%file_name)
        else:
            print("filename is empty!")
        return  ""
    return file_path

def is_hdfs(path):
    return path.startswith("hdfs://")

def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def timmer(fun1):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        res = fun1(*args, **kwargs)
        stop_time = time.time()
        dur = stop_time - start_time
        file_name = kwargs['test_name']
        report_file_path = os.path.join(output_path, file_name + '.txt')
        if to_hdfs:
            mode = "ab" if fs.exists(report_file_path) else "wb"
            with fs.open(report_file_path, mode) as f:
                f.write((file_name + " " + str(dur) + "\n").encode('utf-8'))
        else:
            with open(report_file_path, 'a') as f:
                f.write(file_name + " " + str(dur) + "\n")
        return res

    return wrapper


@timmer
def func_with_timmer(spark, sql, test_name=""):
    df = spark.sql(sql)
    res_name = "res_"+test_name
    df.createOrReplaceTempView(res_name)

    if show_df:
        print("function:", test_name)
        df.show()
        print('-'*10)

    spark.sql("CACHE TABLE %s"%res_name)
    spark.sql("UNCACHE TABLE %s"%res_name)

def test_log(f):
    def wrapper(*args, **kwargs):
        print("--------Start test", f.__name__ + "--------")
        f(*args, **kwargs)
        print("--------Finish test", f.__name__ + "--------")

    return wrapper


def func_template(spark, test_name):
    file_name, schema, sql_ = maps.get(test_name)
    file_path = get_file_path(file_name)
    if not file_path:
        return
    df = spark.read.csv(file_path, sep='|', schema=schema).cache()
    df_name = "df_" + test_name
    df.createOrReplaceTempView(df_name)
    sql = sql_ % df_name
    for i in range(exec_times):
        func_with_timmer(spark, sql, test_name = test_name)
    df.unpersist(blocking=True)

def agg_func_template(spark, test_name):
    agg_func_map = {
        'st_envelope_aggr': envelope_aggr,
        'st_union_aggr': union_aggr,
    }
    agg_func = agg_func_map.get(test_name, None)
    if not callable(agg_func):
        return

    file_name, schema, sql_ = maps.get(test_name)
    file_path = get_file_path(file_name)
    if not file_path:
        return
    df = spark.read.csv(file_path, sep='|', schema=schema).cache()
    df_name = "df_" + test_name
    df.createOrReplaceTempView(df_name)
    sql = sql_ % df_name

    df2 = spark.sql(sql).cache()
    df2_name = "df2_" + test_name
    df2.createOrReplaceTempView(df2_name)
    spark.sql("CACHE TABLE %s"%df2_name)

    @timmer
    def aggfunc_with_timmer(spark, test_name, agg_func=agg_func):
        rdf = agg_func(df2, "geos")
        if show_df:
            print("function_agg:", test_name)
            print(rdf)
            print('-'*10)

    for i in range(exec_times):
        aggfunc_with_timmer(spark, test_name=test_name)

    spark.sql("UNCACHE TABLE %s"%df2_name)
    df2.unpersist(blocking=True)
    df.unpersist(blocking=True)


def parse_args(argv):
    import sys, getopt
    try:
        opts, args = getopt.getopt(argv, "h:p:f:o:t:s:", ["path", "function", "output", "times", "show_df"])
    except getopt.GetoptError as e:
        print("ddd")
        print(str(e))
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()
        elif opt in ("-p", "--path"):
            global data_path
            data_path = arg
        elif opt in ("-f", "--function"):
            global test_name
            test_name = arg.split(',')
        elif opt in ("-o", "--output"):
            global output_path
            output_path = arg
        elif opt in ("-t", "--times"):
            global exec_times
            exec_times = int(arg)
        elif opt in ("-s", "--show_df"):
            global show_df
            try:
                show_df = eval(arg)
            except:
                show_df = False

    global to_hdfs
    to_hdfs = is_hdfs(output_path)
    if is_hdfs(output_path):
        global hdfs_url
        output_path = remove_prefix(output_path, "hdfs://")
        hdfs_url = output_path.split("/", 1)[0]
        output_path = output_path[output_path.find('/'):]
    # report_file_path = os.path.join(output_path, time.strftime("%Y-%m-%d-", time.localtime()) + 'report.txt')


    print("data_path", data_path)
    print("function", test_name)
    print("output_path", output_path)
    print("exec_times", exec_times)
    print("show_df:", show_df)
    return bool(output_path) and bool(data_path)

def print_usage():
    print("Usage\n")
    print(
        """
            -p --path data_path
            -f --function function names ,separated by comma, if empty, test all functions.
            -o --output output path
            -t --times execute times
            -s --show_df whether to show df
        """
        )



def exec_func(spark, test_name):
    agg_func_names = ("st_union_aggr", "st_envelope_aggr")
    func =  agg_func_template if test_name in agg_func_names else func_template
    func(spark, test_name)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2 or not parse_args(sys.argv[1:]):
        print("ooo")
        print_usage()
        sys.exit(1)

    if to_hdfs:
        url, port = hdfs_url.split(':')
        fs = pa.hdfs.connect(url, int(port))
    else:
        fs = os.path
        os.makedirs(output_path, exist_ok=True)

    spark_session = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark profile") \
        .getOrCreate()

    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    register_funcs(spark_session)

    test_name = test_name or maps.keys()
    #test_name = ["st_curvetoline",]
    for test in test_name:
        print("--------Start test", test + " " , exec_times ,  " times" "--------")
        exec_func(spark_session, test)
        print("--------Finish test", test + " " , exec_times ,  " times" "--------")

    spark_session.stop()
