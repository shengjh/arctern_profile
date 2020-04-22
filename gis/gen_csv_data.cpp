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
        "POINT (-121 33.5)",
        "POINT (1.0 2.98760)",
        "POINT (0.987 0.65)",
        "POINT (-0.987 -0.765)",
        "POINT (-1 2)",
        "POINT (180 90)",
        "POINT (1.64 2)",
        "POINT (1 2.2)",
        "POINT (1.0 2.98760)",
        "POINT (0.987 0.65)",
        "POINT (-0.987 -0.765)",
        "POINT (1 2.22)",
        "POINT (-180 -90)",
        "POINT (1.64 2.876)",
        "POINT (1 2.208)",
        "POINT (-77.1234124 23.2327)",

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
        "{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}",
        "{\"type\":\"Point\",\"coordinates\":[125.6, 10.1]}",
        //"{\"coordinates\": [[1.0, 1.0], [3.0, 4.0]], \"type\": \"Multipolygon\"}",
        //"{\"coordinates\": [[1.0, 1.0], [1.0, 2.0], [2.0, 3.0]], \"type\": \"Linestring\"}",
        "{\"coordinates\": [[[0.0, 0.0], [0.0, 4.0], [4.0, 4.0], [0.0, 0.0]]], \"type\": \"Polygon\"}",
        //"{\"coordinates\": [[[[0.0, 0.0], [1.0, -1.0], [1.0, 1.0], [-2.0, 3.0], [0.0, 0.0]]]], \"type\": \"Multipolygon\"}",

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
        "POINT(4 4)|POLYGON((0 6,6 8,8 4,6 0,2 0,0 2,0 6))",
        "POINT(4.2 2.1)|POLYGON((0 6,6 8,8 4,6 0,2 0,0 2,0 6))",
        "POINT(10 1)|POLYGON((0 6,6 8,8 4,6 0,2 0,0 2,0 6))",

};

std::map<std::string, std::vector<std::string>> pools = {
        {"single_polygon",           single_polygon_pool},
        {"double_col",               double_col_pool},
        {"single_col",               single_col_pool},
        {"single_linestring",        single_linestring_pool},
        {"single_point",             single_point_pool},
        {"st_curvetoline",           st_curvetoline_pool},
        {"st_distance",              st_distance_pool},
        {"st_geomfromgeojson",       st_geomfromgeojson_pool},
        {"st_point",                 st_point_pool},
        {"st_polygon_from_envelope", st_polygon_from_envelope_pool},
        {"st_within",                st_within_pool}

};


static void write_data(const std::string &path, const std::string &pattern_name, long rows, long batch_size) {

    if (pools.find(pattern_name) == pools.end()) {
        std::cout << "Failed to generate " << pattern_name << ".csv" << std::endl;
        return;
    }

    std::ofstream file;
    std::string dir_path = path + "/10_" + std::to_string(int(std::log10(rows)));
    auto file_path = dir_path + '/' + pattern_name + ".csv";
    struct stat sb;
    if (stat(dir_path.c_str(), &sb) == -1) {
        mkdir(dir_path.c_str(), 0755);
    }
    file.open(file_path);
    if (file.fail()) {
        std::cout << "Open " << file_path << " error!" << std::endl;
        exit(1);
    }

    auto total = rows;
    std::string batch;
    time_t t;
    srandom(time(&t));
    auto pool = pools[pattern_name];

    /* Since we want to cover pool data as many as possible,
     * write min(total,pool size) rows data as initial data.
     */
    auto init_size = std::min(total, long(pool.size()));
    batch.clear();
    for (auto j = 0; j < init_size; ++j) {
        batch.append(pool[j] + '\n');
    }
    file << batch;

    total -= init_size;
    batch_size = std::min(batch_size, total);
    bool has_remain = (total > 0);

    while (has_remain) {
        batch.clear();
        for (auto j = 0; j != batch_size; ++j) {
            auto i = j % pool.size();
            batch.append(pool[i] + '\n');
        }
        file << batch;
        total -= batch_size;
        has_remain = (total > 0);
    }

    file.close();
    std::cout << "Generate " << pattern_name << ".csv done." << std::endl;
}

static void split(const std::string &s, std::vector<std::string> &tokens, const std::string &delimiters = ",") {
    std::string::size_type lastPos = s.find_first_not_of(delimiters, 0);
    std::string::size_type pos = s.find_first_of(delimiters, lastPos);
    while (std::string::npos != pos || std::string::npos != lastPos) {
        tokens.push_back(s.substr(lastPos, pos - lastPos));
        lastPos = s.find_first_not_of(delimiters, pos);
        pos = s.find_first_of(delimiters, lastPos);
    }
}


static void print_help() {
    std::cout << "Usage:\n";
    std::cout << "\t -r number of rows, requried.\n";
    std::cout << "\t -o output path, required.\n";
    std::cout
            << "\t -p pattern names, requried. sperated by comma, single_polygon,double_col, for example. if specified as 'all', will genenrate all patterns.\n";
    std::cout << "\t -b batch size of per write, optional, default is 100000000.\n";
    std::cout << std::endl;
}

static void print_params(long rows, const std::string &output_path, const std::string &patterns, int batch_size) {
    std::cout << "***************" << std::endl;
    std::cout << "Rows: " << rows << std::endl;
    std::cout << "Patterns: " << patterns << std::endl;
    std::cout << "Output Path: " << output_path << std::endl;
    std::cout << "Batch Size: " << batch_size << std::endl;
    std::cout << "***************" << std::endl;
}

int main(int argc, char *argv[]) {
    int opt;
    long rows;
    bool valid = true;
    int batch_size = 100000000;
    std::string output_path;
    std::string patterns;
#if 1
    while (valid and (opt = getopt(argc, argv, ":r:o:p:b:")) != -1) {
        switch (opt) {
            case 'r': {
                rows = atol(optarg);
                valid = rows > 0;
                break;
            }
            case 'o': {
                output_path = optarg;
                valid = not output_path.empty();
                break;
            }
            case 'p': {
                patterns = optarg;
                valid = not patterns.empty();
                break;
            }
            case 'b': {
                batch_size = atol(optarg);
                valid = batch_size > 0;
                break;
            }
        }
    }
    valid = valid and not(output_path.empty() or patterns.empty());
    if (valid) {
        print_params(rows, output_path, patterns, batch_size);
        std::vector<std::string> pattern_names;
        if (patterns == "all") {
            for (auto it = pools.begin(); it != pools.end(); ++it) {
                pattern_names.push_back(it->first);
            }
        } else {
            split(patterns, pattern_names);
        }
        for (const auto &pattern_name: pattern_names) {
            write_data(output_path, pattern_name, rows, batch_size);
        }
    } else {
        print_help();
    }
#endif
    return 0;
}
