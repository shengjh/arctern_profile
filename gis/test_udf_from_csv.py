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

# add the SPARK_HOME to env
# os.environ["SPARK_HOME"] = "/home/shengjh/Apps/spark-3.0.0-preview2"

data_path = ""
output_path = ""
test_name = []
hdfs_url = ""
to_hdfs = False
fs = None


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
        report_file_path = os.path.join(output_path, args[0] + '.txt')
        if to_hdfs:
            mode = "ab" if fs.exists(report_file_path) else "wb"
            with fs.open(report_file_path, mode) as f:
                f.write((args[0] + " " + str(dur) + "\n").encode('utf-8'))
        else:
            with open(report_file_path, 'a') as f:
                f.write(args[0] + " " + str(dur) + "\n")
        return res

    return wrapper


# @timmer
def calculate_with_timmer(func_name, spark, sql):
    df = spark.sql(sql)
    df.createOrReplaceTempView("df")
    spark.sql("CACHE TABLE df")
    spark.sql("UNCACHE TABLE df")


def calculate(spark, sql):
    df = spark.sql(sql)
    df.createOrReplaceTempView("df")
    spark.sql("CACHE TABLE df")
    spark.sql("UNCACHE TABLE df")


@timmer
def calculate_union_agg_with_timmer(func_name, spark, sql):
    df = spark.sql(sql)
    rdf = union_aggr(df, "geos")


def calculate_union_agg(spark, sql):
    df = spark.sql(sql)
    rdf = union_aggr(df, "geos")


@timmer
def calculate_envelope_agg_with_timmer(func_name, spark, sql):
    df = spark.sql(sql)
    rdf = envelope_aggr(df, "geos")


def calculate_envelope_agg(spark, sql):
    df = spark.sql(sql)
    rdf = envelope_aggr(df, "geos")


def test_log(f):
    def wrapper(*args, **kwargs):
        print("--------Start test", f.__name__ + "--------")
        f(*args, **kwargs)
        print("--------Finish test", f.__name__ + "--------")

    return wrapper


@test_log
def run_st_point(spark):
    file_path = os.path.join(data_path, 'st_point.csv')
    points_df = spark.read.csv(file_path, schema='x double, y double').cache()
    points_df.createOrReplaceTempView("points")
    sql = "select ST_Point(x, y) from points"
    s = time.time()
    calculate_with_timmer('st_point', spark, sql)
    calculate_with_timmer('st_point', spark, sql)
    calculate_with_timmer('st_point', spark, sql)
    e = time.time()
    print("st_point time:", e - s)
    points_df.unpersist(blocking=True)


@test_log
def run_st_point_1(spark):
    file_path = os.path.join(data_path, 'st_point.csv')
    points_df = spark.read.csv(file_path, schema='x double, y double').cache()
    points_df.createOrReplaceTempView("points")
    sql = "select ST_Point(x, y) from points"
    s = time.time()
    for i in range(100):
        calculate_with_timmer('st_point_1', spark, sql)
    e = time.time()
    print("st_point_1 time:", e - s)
    points_df.unpersist(blocking=True)


@test_log
def run_st_geomfromgeojson(spark):
    file_path = os.path.join(data_path, 'st_geomfromgeojson.csv')
    json_df = spark.read.csv(file_path, schema="json string").cache()
    json_df.createOrReplaceTempView("json")
    sql = "select ST_GeomFromGeoJSON(json) from json"
    calculate_with_timmer('st_geomfromgeojson', spark, sql)
    calculate_with_timmer('st_geomfromgeojson', spark, sql)
    calculate_with_timmer('st_geomfromgeojson', spark, sql)
    json_df.unpersist(blocking=True)


@test_log
def run_st_pointfromtext(spark):
    file_path = os.path.join(data_path, 'st_pointfromtext.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_PointFromText(data) from data"
    calculate_with_timmer('st_pointfromtext', spark, sql)
    calculate_with_timmer('st_pointfromtext', spark, sql)
    calculate_with_timmer('st_pointfromtext', spark, sql)
    data_df.unpersist(blocking=True)


@test_log
def run_st_polygonfromtext(spark):
    file_path = os.path.join(data_path, 'st_polygonfromtext.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_PolygonFromText(data) from data"
    calculate_with_timmer('st_polygonfromtext', spark, sql)
    calculate_with_timmer('st_polygonfromtext', spark, sql)
    calculate_with_timmer('st_polygonfromtext', spark, sql)
    data_df.unpersist(blocking=True)


@test_log
def run_st_astext(spark):
    file_path = os.path.join(data_path, 'st_astext.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_AsText(data) from data"
    calculate_with_timmer('st_astext', spark, sql)
    calculate_with_timmer('st_astext', spark, sql)
    calculate_with_timmer('st_astext', spark, sql)
    data_df.unpersist(blocking=True)


@test_log
def run_st_precision_reduce(spark):
    file_path = os.path.join(data_path, 'st_precision_reduce.csv')
    precision_reduce_df = spark.read.csv(file_path, schema="geos string").cache()
    precision_reduce_df.createOrReplaceTempView("precision_reduce")
    sql = "select ST_PrecisionReduce(ST_GeomFromText(geos), 4) from precision_reduce"
    calculate_with_timmer('st_precision_reduce', spark, sql)
    calculate_with_timmer('st_precision_reduce', spark, sql)
    calculate_with_timmer('st_precision_reduce', spark, sql)
    precision_reduce_df.unpersist(blocking=True)


@test_log
def run_st_linestringfromtext(spark):
    file_path = os.path.join(data_path, 'st_linestringfromtext.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_LineStringFromText(data) from data"
    calculate_with_timmer('st_linestringfromtext', spark, sql)
    calculate_with_timmer('st_linestringfromtext', spark, sql)
    calculate_with_timmer('st_linestringfromtext', spark, sql)
    data_df.unpersist(blocking=True)


@test_log
def run_st_geomfromwkt(spark):
    file_path = os.path.join(data_path, 'st_geomfromwkt.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_GeomFromWKT(data) from data"
    calculate_with_timmer('st_geomfromwkt', spark, sql)
    calculate_with_timmer('st_geomfromwkt', spark, sql)
    calculate_with_timmer('st_geomfromwkt', spark, sql)
    data_df.unpersist(blocking=True)


@test_log
def run_st_geomfromtext(spark):
    file_path = os.path.join(data_path, 'st_geomfromtext.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_GeomFromText(data) from data"
    calculate_with_timmer('st_geomfromtext', spark, sql)
    calculate_with_timmer('st_geomfromtext', spark, sql)
    calculate_with_timmer('st_geomfromtext', spark, sql)
    data_df.unpersist(blocking=True)


@test_log
def run_st_intersection(spark):
    file_path = os.path.join(data_path, 'st_intersection.csv')
    intersection_df = spark.read.csv(file_path, schema="left string, right string").cache()
    intersection_df.createOrReplaceTempView("intersection")
    sql = "select ST_Intersection(ST_GeomFromText(left), ST_GeomFromText(right)) from intersection"
    calculate_with_timmer('st_intersection', spark, sql)
    calculate_with_timmer('st_intersection', spark, sql)
    calculate_with_timmer('st_intersection', spark, sql)
    intersection_df.unpersist(blocking=True)


@test_log
def run_st_isvalid(spark):
    file_path = os.path.join(data_path, 'st_isvalid.csv')
    valid_df = spark.read.csv(file_path, schema='geos string').cache()
    valid_df.createOrReplaceTempView("valid")
    sql = "select ST_IsValid(ST_GeomFromText(geos)) from valid"
    calculate_with_timmer('st_isvalid', spark, sql)
    calculate_with_timmer('st_isvalid', spark, sql)
    calculate_with_timmer('st_isvalid', spark, sql)
    valid_df.unpersist(blocking=True)


@test_log
def run_st_equals(spark):
    file_path = os.path.join(data_path, 'st_equals.csv')
    equals_df = spark.read.csv(file_path, schema="left string, right string").cache()
    equals_df.createOrReplaceTempView("equals")
    sql = "select ST_Equals(ST_GeomFromText(left), ST_GeomFromText(right)) from equals"
    calculate_with_timmer('st_equals', spark, sql)
    calculate_with_timmer('st_equals', spark, sql)
    calculate_with_timmer('st_equals', spark, sql)
    equals_df.unpersist(blocking=True)


@test_log
def run_st_touches(spark):
    file_path = os.path.join(data_path, 'st_touches.csv')
    touches_df = spark.read.csv(file_path, schema="left string, right string").cache()
    touches_df.createOrReplaceTempView("touches")
    sql = "select ST_Touches(ST_GeomFromText(left), ST_GeomFromText(right)) from touches"
    calculate_with_timmer('st_touches', spark, sql)
    calculate_with_timmer('st_touches', spark, sql)
    calculate_with_timmer('st_touches', spark, sql)
    touches_df.unpersist(blocking=True)


@test_log
def run_st_overlaps(spark):
    file_path = os.path.join(data_path, 'st_overlaps.csv')
    overlaps_df = spark.read.csv(file_path, schema="left string, right string").cache()
    overlaps_df.createOrReplaceTempView("overlaps")
    sql = "select ST_Overlaps(ST_GeomFromText(left), ST_GeomFromText(right)) from overlaps"
    calculate_with_timmer('st_overlaps', spark, sql)
    calculate_with_timmer('st_overlaps', spark, sql)
    calculate_with_timmer('st_overlaps', spark, sql)
    overlaps_df.unpersist(blocking=True)


@test_log
def run_st_crosses(spark):
    file_path = os.path.join(data_path, 'st_crosses.csv')
    crosses_df = spark.read.csv(file_path, schema="left string, right string").cache()
    crosses_df.createOrReplaceTempView("crosses")
    sql = "select ST_Crosses(ST_GeomFromText(left), ST_GeomFromText(right)) from crosses"
    calculate_with_timmer('st_crosses', spark, sql)
    calculate_with_timmer('st_crosses', spark, sql)
    calculate_with_timmer('st_crosses', spark, sql)
    crosses_df.unpersist(blocking=True)


@test_log
def run_st_issimple(spark):
    file_path = os.path.join(data_path, 'st_issimple.csv')
    simple_df = spark.read.csv(file_path, schema='geos string').cache()
    simple_df.createOrReplaceTempView("simple")
    sql = "select ST_IsSimple(ST_GeomFromText(geos)) from simple"
    calculate_with_timmer('st_issimple', spark, sql)
    calculate_with_timmer('st_issimple', spark, sql)
    calculate_with_timmer('st_issimple', spark, sql)
    simple_df.unpersist(blocking=True)


@test_log
def run_st_geometry_type(spark):
    file_path = os.path.join(data_path, 'st_geometry_type.csv')
    geometry_type_df = spark.read.csv(file_path, schema='geos string').cache()
    geometry_type_df.createOrReplaceTempView("geometry_type")
    sql = "select ST_GeometryType(ST_GeomFromText(geos)) from geometry_type"
    calculate_with_timmer('st_geometry_type', spark, sql)
    calculate_with_timmer('st_geometry_type', spark, sql)
    calculate_with_timmer('st_geometry_type', spark, sql)
    geometry_type_df.unpersist(blocking=True)


@test_log
def run_st_make_valid(spark):
    file_path = os.path.join(data_path, 'st_make_valid.csv')
    make_valid_df = spark.read.csv(file_path, schema='geos string').cache()
    make_valid_df.createOrReplaceTempView("make_valid")
    sql = "select ST_MakeValid(ST_GeomFromText(geos)) from make_valid"
    calculate_with_timmer('st_make_valid', spark, sql)
    calculate_with_timmer('st_make_valid', spark, sql)
    calculate_with_timmer('st_make_valid', spark, sql)
    make_valid_df.unpersist(blocking=True)


@test_log
def run_st_simplify_preserve_topology(spark):
    file_path = os.path.join(data_path, 'st_simplify_preserve_topology.csv')
    simplify_preserve_topology_df = spark.read.csv(file_path, schema='geos string').cache()
    simplify_preserve_topology_df.createOrReplaceTempView("simplify_preserve_topology")
    sql = "select ST_SimplifyPreserveTopology(ST_GeomFromText(geos), 10) from simplify_preserve_topology"
    calculate_with_timmer('st_simplify_preserve_topology', spark, sql)
    calculate_with_timmer('st_simplify_preserve_topology', spark, sql)
    calculate_with_timmer('st_simplify_preserve_topology', spark, sql)
    simplify_preserve_topology_df.unpersist(blocking=True)


@test_log
def run_st_polygon_from_envelope(spark):
    file_path = os.path.join(data_path, 'st_polygon_from_envelope.csv')
    polygon_from_envelope_df = spark.read.csv(file_path,
                                              schema="min_x double, min_y double, max_x double, max_y double").cache()
    polygon_from_envelope_df.createOrReplaceTempView('polygon_from_envelope')
    sql = "select ST_PolygonFromEnvelope(min_x, min_y, max_x, max_y) from polygon_from_envelope"
    calculate_with_timmer('st_polygon_from_envelope', spark, sql)
    calculate_with_timmer('st_polygon_from_envelope', spark, sql)
    calculate_with_timmer('st_polygon_from_envelope', spark, sql)
    polygon_from_envelope_df.unpersist(blocking=True)


@test_log
def run_st_contains(spark):
    file_path = os.path.join(data_path, 'st_contains.csv')
    contains_df = spark.read.csv(file_path, schema="left string, right string").cache()
    contains_df.createOrReplaceTempView("contains")
    sql = "select ST_Contains(ST_GeomFromText(left), ST_GeomFromText(right)) from contains"
    calculate_with_timmer('st_contains', spark, sql)
    calculate_with_timmer('st_contains', spark, sql)
    calculate_with_timmer('st_contains', spark, sql)
    contains_df.unpersist(blocking=True)


@test_log
def run_st_intersects(spark):
    file_path = os.path.join(data_path, 'st_intersects.csv')
    intersects_df = spark.read.csv(file_path, schema="left string, right string").cache()
    intersects_df.createOrReplaceTempView("intersects")
    sql = "select ST_Intersects(ST_GeomFromText(left), ST_GeomFromText(right)) from intersects"
    calculate_with_timmer('st_intersects', spark, sql)
    calculate_with_timmer('st_intersects', spark, sql)
    calculate_with_timmer('st_intersects', spark, sql)
    intersects_df.unpersist(blocking=True)


@test_log
def run_st_within(spark):
    file_path = os.path.join(data_path, 'st_within.csv')
    within_df = spark.read.csv(file_path, schema="left string, right string").cache()
    within_df.createOrReplaceTempView("within")
    sql = "select ST_Within(ST_GeomFromText(left), ST_GeomFromText(right)) from within"
    calculate_with_timmer('st_within', spark, sql)
    calculate_with_timmer('st_within', spark, sql)
    calculate_with_timmer('st_within', spark, sql)
    within_df.unpersist(blocking=True)


@test_log
def run_st_distance(spark):
    file_path = os.path.join(data_path, 'st_distance.csv')
    distance_df = spark.read.csv(file_path, schema="left string, right string").cache()
    distance_df.createOrReplaceTempView("distance")
    sql = "select ST_Distance(ST_GeomFromText(left), ST_GeomFromText(right)) from distance"
    calculate_with_timmer('st_distance', spark, sql)
    calculate_with_timmer('st_distance', spark, sql)
    calculate_with_timmer('st_distance', spark, sql)
    distance_df.unpersist(blocking=True)


@test_log
def run_st_area(spark):
    file_path = os.path.join(data_path, 'st_area.csv')
    area_df = spark.read.csv(file_path, schema='geos string').cache()
    area_df.createOrReplaceTempView("area")
    sql = "select ST_Area(ST_GeomFromText(geos)) from area"
    calculate_with_timmer('st_area', spark, sql)
    calculate_with_timmer('st_area', spark, sql)
    calculate_with_timmer('st_area', spark, sql)
    area_df.unpersist(blocking=True)


@test_log
def run_st_centroid(spark):
    file_path = os.path.join(data_path, 'st_centroid.csv')
    centroid_df = spark.read.csv(file_path, schema='geos string').cache()
    centroid_df.createOrReplaceTempView("centroid")
    sql = "select ST_Centroid(ST_GeomFromText(geos)) from centroid"
    calculate_with_timmer('st_centroid', spark, sql)
    calculate_with_timmer('st_centroid', spark, sql)
    calculate_with_timmer('st_centroid', spark, sql)
    centroid_df.unpersist(blocking=True)


@test_log
def run_st_length(spark):
    file_path = os.path.join(data_path, 'st_length.csv')
    length_df = spark.read.csv(file_path, schema='geos string').cache()
    length_df.createOrReplaceTempView("length")
    sql = "select ST_Length(ST_GeomFromText(geos)) from length"
    calculate_with_timmer('st_length', spark, sql)
    calculate_with_timmer('st_length', spark, sql)
    calculate_with_timmer('st_length', spark, sql)
    length_df.unpersist(blocking=True)


@test_log
def run_st_hausdorffdistance(spark):
    file_path = os.path.join(data_path, 'st_hausdorffdistance.csv')
    hausdorff_df = spark.read.csv(file_path, schema="geo1 string, geo2 string").cache()
    hausdorff_df.createOrReplaceTempView("hausdorff")
    sql = "select ST_HausdorffDistance(ST_GeomFromText(geo1),ST_GeomFromText(geo2)) from hausdorff"
    calculate_with_timmer('st_hausdorffdistance', spark, sql)
    calculate_with_timmer('st_hausdorffdistance', spark, sql)
    calculate_with_timmer('st_hausdorffdistance', spark, sql)
    hausdorff_df.unpersist(blocking=True)


@test_log
def run_st_convexhull(spark):
    file_path = os.path.join(data_path, 'st_convexhull.csv')
    convexhull_df = spark.read.csv(file_path, schema='geos string').cache()
    convexhull_df.createOrReplaceTempView("convexhull")
    sql = "select ST_convexhull(ST_GeomFromText(geos)) from convexhull"
    calculate_with_timmer('st_convexhull', spark, sql)
    calculate_with_timmer('st_convexhull', spark, sql)
    calculate_with_timmer('st_convexhull', spark, sql)
    convexhull_df.unpersist(blocking=True)


@test_log
def run_st_npoints(spark):
    file_path = os.path.join(data_path, 'st_npoints.csv')
    npoints_df = spark.read.csv(file_path, schema='geos string').cache()
    npoints_df.createOrReplaceTempView("npoints")
    sql = "select ST_NPoints(ST_GeomFromText(geos)) from npoints"
    calculate_with_timmer('st_npoints', spark, sql)
    calculate_with_timmer('st_npoints', spark, sql)
    calculate_with_timmer('st_npoints', spark, sql)
    npoints_df.unpersist(blocking=True)


@test_log
def run_st_envelope(spark):
    file_path = os.path.join(data_path, 'st_envelope.csv')
    envelope_df = spark.read.csv(file_path, schema='geos string').cache()
    envelope_df.createOrReplaceTempView("envelope")
    sql = "select ST_Envelope(ST_GeomFromText(geos)) from envelope"
    calculate_with_timmer('st_envelope', spark, sql)
    calculate_with_timmer('st_envelope', spark, sql)
    calculate_with_timmer('st_envelope', spark, sql)
    envelope_df.unpersist(blocking=True)


@test_log
def run_st_buffer(spark):
    file_path = os.path.join(data_path, 'st_buffer.csv')
    buffer_df = spark.read.csv(file_path, schema='geos string').cache()
    buffer_df.createOrReplaceTempView("buffer")
    sql = "select ST_Buffer(ST_GeomFromText(geos), 1.2) from buffer"
    calculate_with_timmer('st_buffer', spark, sql)
    calculate_with_timmer('st_buffer', spark, sql)
    calculate_with_timmer('st_buffer', spark, sql)
    buffer_df.unpersist(blocking=True)


@test_log
def run_st_union_aggr(spark):
    file_path = os.path.join(data_path, 'st_union_aggr.csv')
    union_aggr_df1 = spark.read.csv(file_path, schema='geos string').cache()
    union_aggr_df1.createOrReplaceTempView("union_aggr1")
    sql = "select ST_GeomFromText(geos) as geos from union_aggr1"
    calculate_union_agg_with_timmer('st_union_aggr', spark, sql)
    calculate_union_agg_with_timmer('st_union_aggr', spark, sql)
    calculate_union_agg_with_timmer('st_union_aggr', spark, sql)
    union_aggr_df1.unpersist(blocking=True)


@test_log
def run_st_envelope_aggr(spark):
    file_path = os.path.join(data_path, 'st_envelope_aggr.csv')
    envelope_aggr_df = spark.read.csv(file_path, schema='geos string')
    envelope_aggr_df.createOrReplaceTempView('envelope_aggr')
    sql = "select ST_GeomFromText(geos) as geos from envelope_aggr"
    calculate_envelope_agg_with_timmer('st_envelope_aggr', spark, sql)
    calculate_envelope_agg_with_timmer('st_envelope_aggr', spark, sql)
    calculate_envelope_agg_with_timmer('st_envelope_aggr', spark, sql)
    envelope_aggr_df.unpersist(blocking=True)


@test_log
def run_st_transform(spark):
    file_path = os.path.join(data_path, 'st_transform.csv')
    buffer_df = spark.read.csv(file_path, schema='geos string').cache()
    buffer_df.createOrReplaceTempView("buffer")
    sql = "select ST_Transform(ST_GeomFromText(geos), 'epsg:4326', 'epsg:3857') from buffer"
    calculate_with_timmer('st_transform', spark, sql)
    calculate_with_timmer('st_transform', spark, sql)
    calculate_with_timmer('st_transform', spark, sql)
    buffer_df.unpersist(blocking=True)


@test_log
def run_st_curvetoline(spark):
    file_path = os.path.join(data_path, 'st_curvetoline.csv')
    buffer_df = spark.read.csv(file_path, schema='geos string').cache()
    buffer_df.createOrReplaceTempView("buffer")
    sql = "select ST_CurveToLine(ST_GeomFromText(geos)) from buffer"
    calculate_with_timmer('st_curvetoline', spark, sql)
    calculate_with_timmer('st_curvetoline', spark, sql)
    calculate_with_timmer('st_curvetoline', spark, sql)
    buffer_df.unpersist(blocking=True)


def parse_args(argv):
    import sys, getopt
    try:
        opts, args = getopt.getopt(argv, "h:p:f:o:", ["path", "function", "output"])
    except getopt.GetoptError:
        print('python test_udf_from_csv.py -p <data path> -f <function name, default all udf functions> -o <output>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(
                'python test_udf_from_csv.py -p <data path> -f <function name, default all udf functions> -o <output>')
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
    global to_hdfs
    to_hdfs = is_hdfs(output_path)
    if is_hdfs(output_path):
        global hdfs_url
        output_path = remove_prefix(output_path, "hdfs://")
        hdfs_url = output_path.split("/", 1)[0]
        output_path = output_path[output_path.find('/'):]
    # report_file_path = os.path.join(output_path, time.strftime("%Y-%m-%d-", time.localtime()) + 'report.txt')


if __name__ == "__main__":
    parse_args(sys.argv[1:])
    if to_hdfs:
        url, port = hdfs_url.split(':')
        fs = pa.hdfs.connect(url, int(port))
    else:
        os.makedirs(output_path, exist_ok=True)

    spark_session = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark profile") \
        .getOrCreate()

    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    register_funcs(spark_session)

    funcs = {
        'st_point': run_st_point,
        'st_point_1': run_st_point_1,
        'st_intersection': run_st_intersection,
        'st_isvalid': run_st_isvalid,
        'st_equals': run_st_equals,
        'st_touches': run_st_touches,
        'st_overlaps': run_st_overlaps,
        'st_crosses': run_st_crosses,
        'st_issimple': run_st_issimple,
        'st_geometry_type': run_st_geometry_type,
        'st_make_valid': run_st_make_valid,
        'st_simplify_preserve_topology': run_st_simplify_preserve_topology,
        'st_polygon_from_envelope': run_st_polygon_from_envelope,
        'st_contains': run_st_contains,
        'st_intersects': run_st_intersects,
        'st_within': run_st_within,
        'st_distance': run_st_distance,
        'st_area': run_st_area,
        'st_centroid': run_st_centroid,
        'st_length': run_st_length,
        'st_hausdorffdistance': run_st_hausdorffdistance,
        'st_convexhull': run_st_convexhull,
        'st_npoints': run_st_npoints,
        'st_envelope': run_st_envelope,
        'st_buffer': run_st_buffer,
        'st_union_aggr': run_st_union_aggr,
        'st_envelope_aggr': run_st_envelope_aggr,
        'st_transform': run_st_transform,
        'st_curvetoline': run_st_curvetoline,
        'st_geomfromgeojson': run_st_geomfromgeojson,
        'st_pointfromtext': run_st_pointfromtext,
        'st_polygonfromtext': run_st_polygonfromtext,
        'st_linestringfromtext': run_st_linestringfromtext,
        'st_geomfromwkt': run_st_geomfromwkt,
        'st_geomfromtext': run_st_geomfromtext,
        'st_astext': run_st_astext,
    }

    test_name = test_name or funcs.keys()
    for test in test_name:
        funcs[test](spark_session)

    spark_session.stop()
