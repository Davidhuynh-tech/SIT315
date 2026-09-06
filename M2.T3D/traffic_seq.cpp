// Sequential Traffic Control Simulator (M2.T3D)
// Usage: traffic_seq <file> [topN] [--max-print K]
// Build: g++ -O2 -std=c++17 traffic_seq.cpp -o traffic_seq

#include "traffic_common.hpp"

#include <chrono>
#include <fstream>

// Main function for the sequential traffic control simulator.
int main(int argc, char** argv) {
    if (argc < 2) die("Usage: traffic_seq <file> [topN] [--max-print K] [--out file]");
    const std::string path = argv[1];
    const int top_n = (argc >= 3 && argv[2][0] != '-') ? std::stoi(argv[2]) : 5;
    const int max_print = flag_int(argc, argv, "--max-print", -1);
    const char* out_flag = flag_cstr(argc, argv, "--out");
    const std::string outfile = out_flag ? out_flag : default_report_name(path, "_seq");

    // Start timing and open the input file
    auto t0 = std::chrono::steady_clock::now();
    std::ifstream in(path);
    if (!in) die("Could not open " + path);
    
    // Initialize the hour table and record count
    HourTable table;
    long long nrec = 0;
    auto take = [&](const std::string& row) {
        Record r = parse_line(row);
        if (r.light > 0) {
            add_record(table, r);
            ++nrec;
        }
    };
    
    // Read the first line to check for a header and process it if it's valid.
    std::string line;
    if (!std::getline(in, line)) die("File is empty: " + path);
    if (parse_line(line).light > 0) take(line);
    while (std::getline(in, line)) {
        take(line);
        if (nrec > 0 && nrec % 2000000 == 0) {
            std::cerr << "seq: processed " << nrec << " rows\n";
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    const double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    std::ostringstream report;
    report << "Mode: sequential\n";
    report << "Input: " << path << "\n";
    report << "Records: " << nrec << "  Hours: " << table.size() << "  TopN: " << top_n
           << "\n";
    report << "Checksum: " << checksum(table) << "\n";
    report << "Time_ms: " << ms << "\n";
    print_top_n(table, top_n, max_print, report);
    print_and_save(report.str(), outfile);
    return 0;
}
