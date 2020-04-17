import os
import sys
import pyarrow as pa

row_per_batch = 10000000
rows = 1
to_hdfs = False
output_path = ""
test_name = []
hdfs_url = ""
fs = None


def is_hdfs(path):
    return path.startswith("hdfs://")


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def parse_args(argv):
    import getopt
    try:
        opts, args = getopt.getopt(argv, "hr:p:f:", ["rows=", "path", "function"])
    except getopt.GetoptError:
        print('python gen_csv_data.py -r <rows> -p <output path> -f <function name>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('python gen_csv_data.py -r <rows> -p <output path> -f <function name>')
            sys.exit()
        elif opt in ("-r", "--rows"):
            global rows
            rows = int(arg)
            global row_per_batch
            if rows < row_per_batch:
                row_per_batch = rows
        elif opt in ("-p", "--path"):
            global to_hdfs, output_path
            output_path = arg
            to_hdfs = is_hdfs(output_path)
        elif opt in ("-f", "--function"):
            global test_name
            test_name = arg.split(',')
    from math import log, ceil
    output_path = os.path.join(output_path, '10_' + str(ceil(log(rows, 10))))
    if to_hdfs:
        global hdfs_url
        output_path = remove_prefix(output_path, "hdfs://")
        hdfs_url = output_path.split("/", 1)[0]
        output_path = output_path[output_path.find('/'):]


class _OneColDecorator(object):
    def __init__(self, f, line):
        self._line = line
        self._file_name = f.__name__[4:] + '.csv'

    def __call__(self):
        def df_to_writer(writer):
            import gc
            total = rows
            geos = [self._line] * min(total, row_per_batch)
            geos_str = '\n'.join(geos)
            del geos
            gc.collect()
            has_remain = total > 0
            while has_remain:
                if total < row_per_batch:
                    geos = [self._line] * total
                    geos_str = '\n'.join(geos)
                    del geos
                    gc.collect()

                total -= row_per_batch
                has_remain = total > 0
                new_line = '\n' if has_remain else ''
                writer.write((geos_str + new_line).encode('utf-8'))

        file = os.path.join(output_path, self._file_name)
        if to_hdfs:
            with fs.open(file, "wb") as writer:
                df_to_writer(writer)
        else:
            with open(file, "wb") as writter:
                df_to_writer(writter)
        print("Generate ", self._file_name, " done.")


def OneColDecorator(f=None, line=''):
    if f:
        return _OneColDecorator(f)
    else:
        def wrapper(f):
            return _OneColDecorator(f, line)

        return wrapper


class _TwoColDecorator(_OneColDecorator):
    def __init__(self, f, left, right):
        self._line = str(left) + '|' + str(right)
        self._file_name = f.__name__[4:] + '.csv'


def TwoColDecorator(f=None, left='', right=''):
    if f:
        return _TwoColDecorator(f)
    else:
        def wrapper(f):
            return _TwoColDecorator(f, left, right)

        return wrapper


@TwoColDecorator(left=0.1, right=0.2)
def gen_st_point():
    pass


@TwoColDecorator(left='POINT(0 0)', right='LINESTRING ( 2 0, 0 2 )')
def gen_st_intersection():
    pass


@OneColDecorator(line='POINT (30 10)')
def gen_st_isvalid():
    pass


@TwoColDecorator(left='LINESTRING(0 0, 10 10)', right='LINESTRING(0 0, 5 5, 10 10)')
def gen_st_equals():
    pass


@TwoColDecorator(left='LINESTRING(0 0, 1 1, 0 2)', right='POINT(1 1)')
def gen_st_touches():
    pass


@TwoColDecorator(left='POLYGON((1 1, 4 1, 4 5, 1 5, 1 1))', right='POLYGON((3 2, 6 2, 6 6, 3 6, 3 2))')
def gen_st_overlaps():
    pass


@TwoColDecorator(left='MULTIPOINT((1 3), (4 1), (4 3))', right='POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))')
def gen_st_crosses():
    pass


@OneColDecorator(line='POLYGON((1 2, 3 4, 5 6, 1 2))')
def gen_st_issimple():
    pass


@OneColDecorator(line='LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)')
def gen_st_geometry_type():
    pass


@OneColDecorator(line='LINESTRING(0 0, 10 0, 20 0, 20 0, 30 0)')
def gen_st_make_valid():
    pass


@OneColDecorator(
    line='POLYGON((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33, 8 25))')
def gen_st_simplify_preserve_topology():
    pass


@OneColDecorator(
    line='1.0|3.3|5.1|7.0'
)
def gen_st_polygon_from_envelope():
    pass


@TwoColDecorator(
    left='POLYGON((-1 3,2 1,0 -3,-1 3))', right='POLYGON((0 2,1 1,0 -1,0 2))'
)
def gen_st_contains():
    pass


@TwoColDecorator(
    left='POINT(0 0)', right='LINESTRING ( 0 0, 0 2 )'
)
def gen_st_intersects():
    pass


@TwoColDecorator(
    left='POINT (1 8)', right='POLYGON ((0 0,0 4,2 2,4 4,4 0,0 0))'
)
def gen_st_within():
    pass


@TwoColDecorator(
    left='POINT(-10 55)', right='POINT(-10 40)'
)
def gen_st_distance():
    pass


@OneColDecorator(
    line='MULTIPOLYGON (((1 1,1 2,2 2,2 1,1 1)),((0 0,1 -1,1 1,-2 3,0 0)))'
)
def gen_st_area():
    pass


@OneColDecorator(line='MULTIPOINT ( -1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6 )')
def gen_st_centroid():
    pass


@OneColDecorator(line='LINESTRING(-72.12 42.45, -72.12 42.45, -72.12 42.15)')
def gen_st_length():
    pass


@TwoColDecorator(
    left='POLYGON((0 0 ,0 1, 1 1, 1 0, 0 0))', right='POLYGON((0 0 ,0 2, 1 1, 1 0, 0 0))'
)
def gen_st_hausdorffdistance():
    pass


@OneColDecorator(line='GEOMETRYCOLLECTION(LINESTRING(2.5 3,-2 1.5), POLYGON((0 1,1 3,1 -2,0 1)))')
def gen_st_convexhull():
    pass


@OneColDecorator(line='LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)')
def gen_st_npoints():
    pass


@OneColDecorator(line='POLYGON ((0 0,0 4,2 2,4 4,4 0,0 0))')
def gen_st_envelope():
    pass


@OneColDecorator(line='POLYGON((0 0,1 0,1 1,0 0))')
def gen_st_buffer():
    pass


@OneColDecorator(line='POLYGON ((1 1,1 2,2 2,2 1,1 1))')
def gen_st_union_aggr():
    pass


@OneColDecorator(line='POLYGON ((0 0,4 0,4 4,0 4,0 0))')
def gen_st_envelope_aggr():
    pass


@OneColDecorator(line='POINT (10 10)')
def gen_st_transform():
    pass


@OneColDecorator(line='CURVEPOLYGON(CIRCULARSTRING(0 0, 4 0, 4 4, 0 4, 0 0))')
def gen_st_curvetoline():
    pass


@OneColDecorator(line="{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}")
def gen_st_geomfromgeojson():
    pass


@OneColDecorator(line='POINT (30 10)')
def gen_st_pointfromtext():
    pass


@OneColDecorator(line='POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))')
def gen_st_polygonfromtext():
    pass


@OneColDecorator(line='LINESTRING (0 0, 0 1, 1 1, 1 0)')
def gen_st_linestringfromtext():
    pass


@OneColDecorator(line='POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))')
def gen_st_geomfromwkt():
    pass


@OneColDecorator(line='POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))')
def gen_st_geomfromtext():
    pass


@OneColDecorator(line='POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))')
def gen_st_astext():
    pass


funcs = {
    'st_point': gen_st_point,
    'st_intersection': gen_st_intersection,
    'st_isvalid': gen_st_isvalid,
    'st_equals': gen_st_equals,
    'st_touches': gen_st_touches,
    'st_overlaps': gen_st_overlaps,
    'st_crosses': gen_st_crosses,
    'st_issimple': gen_st_issimple,
    'st_geometry_type': gen_st_geometry_type,
    'st_make_valid': gen_st_make_valid,
    'st_simplify_preserve_topology': gen_st_simplify_preserve_topology,
    'st_polygon_from_envelope': gen_st_polygon_from_envelope,
    'st_contains': gen_st_contains,
    'st_intersects': gen_st_intersects,
    'st_within': gen_st_within,
    'st_distance': gen_st_distance,
    'st_area': gen_st_area,
    'st_centroid': gen_st_centroid,
    'st_length': gen_st_length,
    'st_hausdorffdistance': gen_st_hausdorffdistance,
    'st_convexhull': gen_st_convexhull,
    'st_npoints': gen_st_npoints,
    'st_envelope': gen_st_envelope,
    'st_buffer': gen_st_buffer,
    'st_union_aggr': gen_st_union_aggr,
    'st_envelope_aggr': gen_st_envelope_aggr,
    'st_transform': gen_st_transform,
    'st_curvetoline': gen_st_curvetoline,
    'st_geomfromgeojson': gen_st_geomfromgeojson,
    'st_pointfromtext': gen_st_pointfromtext,
    'st_polygonfromtext': gen_st_polygonfromtext,
    'st_linestringfromtext': gen_st_linestringfromtext,
    'st_geomfromwkt': gen_st_geomfromwkt,
    # 'st_geomfromtext': gen_st_geomfromtext,
    'st_astext': gen_st_astext,
}

if __name__ == "__main__":
    parse_args(sys.argv[1:])
    if to_hdfs:
        url, port = hdfs_url.split(':')
        fs = pa.hdfs.connect(url, int(port))
    else:
        os.makedirs(output_path, exist_ok=True)
    test_name = test_name or funcs.keys()
    for name in test_name:
        funcs[name]()
