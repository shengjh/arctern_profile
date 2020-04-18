#include <iostream>
#include <getopt.h>
#include <string>
#include <vector>
#include <fstream>
#include <map>
#include <functional>
#include <math.h>
#include <sys/types.h>
#include <sys/stat.h>


int rows = 0;
int batch_size = 100000000;
std::string path;
std::string type;


const std::vector<std::string> single_polygon_pool = {
        "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
        "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))",
        "POLYGON ((0 0, 0 2, 2 0, 0 0))",
        "POLYGON ((1.2 4.1, 5.1 5.8, 6.8 1.9, 2.9 0.2, 1.2 4.1))",
        "POLYGON ((4.8 5.8, 1.6 4.9, 1.4 1.3, 4.7 0.1, 7 2.3, 4.8 5.8))",
        "POLYGON ((0 4, 2 8, 6 8, 10 6, 8 2, 4 0, 0 2, 0 4))",
        "POLYGON ((0 2, 0 4, 2 6, 6 6, 8 4, 8 2, 6 0, 2 0, 0 2))",
        "POLYGON ((0 2, -2 4, -2 6, 4 8, 8 8, 10 6, 10 2, 8 0, 4 0, 0 2))",

};


const std::vector<std::string> double_col_pool = {
        "POLYGON ((1 1,1 2,2 2,2 1,1 1))|MULTIPOINT (3 4)",
        "POLYGON ((1 1,1 2,2 2,2 1,1 1))|LINESTRING (1 1,1 2,2 3)",
        "POLYGON ((1 1,1 2,2 2,2 1,1 1))|LINESTRING (1 1,1 2,2 3,1 1)",
        "POLYGON ((1 1,1 2,2 2,2 1,1 1))|MULTILINESTRING ((1 1,1 2),(2 4,1 9,1 8))",
        "POLYGON ((1 1,1 5,5 5,1 1))|POLYGON ((2 2,2 3,3 3,2 2))",
        "POINT(5 5)|LINESTRING (0 0, 10 10)",
        "POINT(7 9)|LINESTRING (0 0, 10 10)",
        "POINT(7.5 7.5)|LINESTRING (0 0, 10 10)",
        "POINT (1 8)|MULTIPOINT (1 1,3 4)",
        "MULTIPOINT (1 1,3 4)|POINT (1 8)",
        "MULTIPOINT (1 1,3 4)|MULTIPOINT (1 1,3 4)",
        "MULTIPOINT (1 1,3 4)|MULTILINESTRING ((1 1,3 4,1 1))",
        "POINT (1 8)|POLYGON ((1 1,1 2,2 2,2 1,1 1))",
        "POINT (1 8)|MULTIPOLYGON (((0 0,1 -1,1 1,-2 3,0 0)))",
        "MULTIPOINT (1 1,3 4)|POLYGON ((0 0,0 4,4 4,0 0))",
        "MULTIPOINT (1 1,3 4)|MULTIPOLYGON (((0 0,1 -1,1 1,-2 3,0 0)))",
        "MULTILINESTRING ((1 1,1 2),(2 4,1 9,1 8))|MULTIPOINT (1 1,3 4)",
        "MULTIPOINT (3 4)|MULTILINESTRING ((1 1,1 2),(2 4,1 9,1 8))",
        "LINESTRING (1 1,1 2,2 3)|MULTILINESTRING ((1 1,1 2),(2 4,1 9,1 8))",
        "LINESTRING (1 1,1 2,2 3,1 1)|MULTIPOINT (3 4)",
        "LINESTRING (1 1,1 2,2 3,1 1)|LINESTRING (1 1,1 2,2 3)",
        "LINESTRING (1 1,1 2,2 3,1 1)|LINESTRING (1 1,1 2,2 3,1 1)",

};

const std::vector<std::string> single_col_pool = {
        "POINT (30 10)",
        "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))",
        "LINESTRING (1 1,2 2,2 3.5,1 3,1 2,2 1)",
        "MULTIPOINT(0 0, 7 7)",
        "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING( 1 1 , 2 2, 3 3))",
        "MULTIPOINT (1 1,3 4)",
        "MULTIPOINT (3 4)",
        "LINESTRING (1 1,1 2,2 3)",
        "LINESTRING (1 1,1 2,2 3,1 1)",
        "MULTILINESTRING ((1 1,1 2),(2 4,1 9,1 8))",
        "MULTILINESTRING ((2 4,1 9,1 8))",
        "MULTILINESTRING ((1 1,3 4,1 1))",
        "POLYGON ((1 1,1 2,2 2,2 1,1 1))",
        "POLYGON ((0 0,0 4,2 2,4 4,4 0,0 0))",
        "POLYGON ((0 0,0 4,4 4,0 0))",
        "MULTIPOLYGON (((0 0,1 -1,1 1,-2 3,0 0)))",
        "MULTIPOLYGON (((1 1,1 2,2 2,2 1,1 1)),((0 0,1 -1,1 1,-2 3,0 0)))",
};
const std::vector<std::string> single_linestring_pool = {
        "LINESTRING (1 1,2 2,2 3.5,1 3,1 2,2 1)",
        "LINESTRING (1 2,2 3,1 2,4 6,2 2,1 6)",
        "LINESTRING (0.9876 0,0 2.9876)",
        "LINESTRING (1 1,2 2.7654,2.5431 3.5,1.666 3,1 2,2 1)",
        "LINESTRING (0 0,0 2)",
        "LINESTRING (1 2,2 3.121,1 2,4 6,2 2,1 6)",
        "LINESTRING (0.121 0,0 2.9876)",
        "LINESTRING (1 1,2 2,2 3.5,1 3,1 2,2 1)",
        "LINESTRING (0 0,1 1,2 2,3 3,4 4)",
        "LINESTRING (0 0,1 1,2 2,3 3,4 4,5 5)",
        "LINESTRING (0 1,1 2,2 3,3 3,4 4,5 5)",
        "LINESTRING (0 1,1 2,2 3,3 4,4 5,5 6)",
        "LINESTRING (0 3,1 5,2 3,3 4,4 5,5 6)",
        "LINESTRING (1 2,2 3,1 2,4 6,2 2,1 7)",
        "LINESTRING (0 0,0 1,1 1,1 0,0 0)",
        "LINESTRING (0 0,0 1,1 1,1 0,0.5 0,0 0)",
        "LINESTRING (0 0,1 11)",
        "LINESTRING (0 0,1 12)",

};
const std::vector<std::string> single_point_pool = {
        "POINT (0.987 -0.765)",
        "POINT (1 2)",
        "POINT (1 22)",
        "POINT (1.64 2)",
        "POINT (1 2.2)",
        "POINT (11321 231123)",
        "POINT (1.0 2.98760)",
        "POINT (0.987 0.65)",
        "POINT (-0.987 -0.765)",
        "POINT (-1 2)",
        "POINT (1132321 23123123)",
        "POINT (1.64 2)",
        "POINT (1 2.2)",
        "POINT (1.0 2.98760)",
        "POINT (0.987 0.65)",
        "POINT (-0.987 -0.765)",
        "POINT (1 2.22)",
        "POINT (113232.1 23.123123)",
        "POINT (1.64 2.876)",
        "POINT (1 2.208)",
        "POINT (1131.326412 23123.2327)",

};
const std::vector<std::string> st_curvetoline_pool = {
        "CURVEPOLYGON(CIRCULARSTRING(0 0, 4 0, 4 4, 0 4, 0 0))",
        "CIRCULARSTRING(0 0, 3 3, 0 0)",
        "CIRCULARSTRING(0 0, 2 2, 4 4)",
        "CIRCULARSTRING (0 2, -1 1,0 0, 0.5 0, 1 0, 2 1, 1 2, 0.5 2, 0 2)",
        "CIRCULARSTRING (0 2, -1 1, 0 2)",
        "COMPOUNDCURVE(CIRCULARSTRING(0 2, -1 1,1 0),CIRCULARSTRING( 1 0, 2 1, 1 2),(1 2, 0.5 2, 0 2))",
        "COMPOUNDCURVE(LINESTRING(0 2, -1 1,1 0),CIRCULARSTRING( 1 0, 2 1, 1 2),(1 2, 0.5 2, 0 2))",
        "COMPOUNDCURVE(CIRCULARSTRING(0 0, 1 1, 1 0),(1 0, 0 1))",
        "COMPOUNDCURVE(LINESTRING(0 0, 1 1, 1 0))",

};
const std::vector<std::string> st_distance_pool = {
        "POINT(-10 55)|POINT(-10 40)",
        "POINT(-2 5)|POINT(-1 4)",
        "POINT(-7 25)|POINT(-10 40)",
        "POINT(-12 35)|POINT(-12 42)",
        "POINT(-1 5)|POINT(-13 43)",
        "POINT(-40 25)|POINT(-40 10)",
        "POINT(-30 15)|POINT(-20 40)",
        "POINT(-10.0 25)|POINT(-11 20)",

};
const std::vector<std::string> st_geomfromgeojson_pool = {
        "{'type':'Polygon','coordinates':[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}",
        "{'TYPE':'POINT','COORDINATES':[125.6, 10.1]}",
        "{'COORDINATES': [[1.0, 1.0], [3.0, 4.0]], 'TYPE': 'MULTIPOINT'}",
        "{'COORDINATES': [[1.0, 1.0], [1.0, 2.0], [2.0, 3.0]], 'TYPE': 'LINESTRING'}",
        "{'COORDINATES': [[[0.0, 0.0], [0.0, 4.0], [4.0, 4.0], [0.0, 0.0]]], 'TYPE': 'POLYGON'}",
        "{'COORDINATES': [[[[0.0, 0.0], [1.0, -1.0], [1.0, 1.0], [-2.0, 3.0], [0.0, 0.0]]]], 'TYPE': 'MULTIPOLYGON'}",

};

const std::vector<std::string> st_point_pool = {
        "12.5676|17.7897",
        "12.5678|17.7895",
        "2.5676|17.7897",
        "12.5678|7.7895",
        "-12.5676|17.7897",
        "12.5678|-17.7895",
        "-2.5676|-17.7897",
        "6.5678|9.7895",
        "-6.5678|9.7895",
        "6.5678|-9.7895",
        "-6.5678|-9.7895",
        "9.0|6.0",
        "9.0|-6.0",
        "-9.0|6.0",
        "-9.0|-6.0",
        "9.5|6.0",
        "9.5|-6.0",
        "-9.5|6.0",
        "-9.5|-6.0",
        "9|5",
        "18|8",
        "16.9807|16.987",
        "0|0",
        "0.0|0",
        "8.0|10.0",
        "8.0|6.0",
        "10.0|1.0",
        "5.0|10.0",
        "3.0|5.0",
        "7.0|3.0",
        "45.747872|96.193504",
        "21.507721|37.289151",
        "73.388003|81.457667",
        "52.096912|21.554577",
        "80.335055|10.929492",
        "51.879734|16.702802",
};

const std::vector<std::string> st_polygon_from_envelope_pool = {
        "10.1|19.7|91.9|98.3",
        "16.1|16.6|93.3|94.0",
        "11.0|18.7|88.3|98.2",
        "13.9|19.1|82.2|83.4",
        "12.0|16.2|81.5|90.6",
        "10.4|11.7|87.5|92.2",
        "15.5|18.6|88.7|98.4",
        "14.8|16.9|83.0|85.6",
        "10.8|16.5|83.9|84.4",
        "12.5|14.8|80.8|97.1",

};

const std::vector<std::string> st_within_pool = {
        "POINT(0 2)|POLYGON((0 6,6 8,8 4,6 0,2 0,0 2,0 6))",
        "POINT(-2 2)|POLYGON((0 6,6 8,8 4,6 0,2 0,0 2,0 6))",
        "POINT(4 4)|(POLYGON((0 6,6 8,8 4,6 0,2 0,0 2,0 6))",
        "POINT(4.2, 2.1)|POLYGON((0 6,6 8,8 4,6 0,2 0,0 2,0 6))",
        "POINT(10 1)|POLYGON((0 6,6 8,8 4,6 0,2 0,0 2,0 6))",

};

std::map<std::string, std::vector<std::string>> pools = {
        {"single_polygon", single_polygon_pool},
        {"double_col", double_col_pool},
        {"single_col", single_col_pool},
        {"single_linestring", single_linestring_pool},
        {"single_point", single_point_pool},
        {"st_curvetoline", st_curvetoline_pool},
        {"st_distance", st_distance_pool},
        {"st_geomfromgeojson", st_geomfromgeojson_pool},
        {"st_point", st_point_pool},
        {"st_polygon_from_envelope", st_polygon_from_envelope_pool},
        {"st_within", st_within_pool}

};


void write_data(const std::string &path, const std::string &type_name) {
    std::ofstream file;
    std::string dir_path = path + "/10_" + std::to_string(int(std::log10(rows)));
    auto file_path = dir_path + '/' + type_name + ".csv";
    struct stat sb;
    if (stat(dir_path.c_str(), &sb) == -1) {
        mkdir(dir_path.c_str(), 0755);
    }
    file.open(file_path);
    if (file.fail()) {
        std::cout << "Open " << file_path << " error!" << std::endl;
        exit(1);
    }

    int total = rows;
    std::string batch;
    time_t t;
    srandom(time(&t));
    if (total < batch_size) {
        batch_size = total;
    }
    auto pool = pools[type_name];
    bool has_remain = (total > 0);
    batch.reserve(batch_size * 15);
    while (has_remain) {
        batch.clear();
        for (int j = 0; j < batch_size; ++j) {
            auto i = random() % pool.size();
            batch.append(pool[i] + '\n');
        }
        file << batch;

        total -= batch_size;
        has_remain = (total > 0);
    }


    file.close();
    std::cout << "Generate " << type << ".csv done.";
}


int main(int argc, char *argv[]) {
    int opt;

    while ((opt = getopt(argc, argv, ":r:p:f:")) != -1) {
        switch (opt) {
            case 'r': {
                rows = atol(optarg);
                break;
            }
            case 'p': {
                path = optarg;
                if (path.empty()) exit(1);
                break;
            }
            case 'f': {
                type = optarg;
                if (type.empty()) exit(1);
            }
            default:
                break;
        }
    }

    write_data(path, type);
    return 0;
}