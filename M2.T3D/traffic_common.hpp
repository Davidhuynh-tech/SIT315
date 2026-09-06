// Shared parse / hour table / print for sequential and producer-consumer.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

struct Record {
    std::string hour;
    int light = 0;
    int cars = 0;
};

using LightTotals = std::unordered_map<int, long long>;
using HourTable = std::map<std::string, LightTotals>;

inline void die(const std::string& msg) {
    std::cerr << msg << "\n";
    std::exit(1);
}

inline void merge_hours(HourTable& dst, const HourTable& src) {
    for (const auto& [hour, lights] : src) {
        LightTotals& slot = dst[hour];
        for (const auto& [id, cars] : lights) slot[id] += cars;
    }
}

inline Record parse_line(const std::string& line) {
    Record r;
    std::string raw = line;
    if (!raw.empty() && raw.back() == '\r') raw.pop_back();
    if (raw.empty()) return r;

    std::vector<std::string> f;
    std::stringstream ss(raw);
    std::string cell;
    while (std::getline(ss, cell, ',')) f.push_back(cell);

    std::string ts, light_s, cars_s;
    if (f.size() >= 4) {
        ts = f[1];
        light_s = f[2];
        cars_s = f[3];
    } else if (f.size() >= 3) {
        ts = f[0];
        light_s = f[1];
        cars_s = f[2];
    } else {
        return r;
    }

    auto all_digits = [](const std::string& s) {
        if (s.empty()) return false;
        for (char c : s) {
            if (c < '0' || c > '9') return false;
        }
        return true;
    };

    if (ts.size() >= 13 && ts[4] == '-') {
        r.hour = ts.substr(0, 13);
    } else if (ts.size() >= 2 && ts[2] == ':') {
        r.hour = ts.substr(0, 2);
    } else if (all_digits(ts)) {
        const long mins = std::stol(ts);
        const int hidx = static_cast<int>(mins / 60);
        char buf[16];
        std::snprintf(buf, sizeof(buf), "%04d", hidx);
        r.hour = buf;
    } else if (ts.size() >= 2) {
        r.hour = ts.substr(0, 2);
    } else {
        r.hour = ts;
    }
    try {
        std::string id = light_s;
        if (id.size() >= 3 && (id[0] == 'T' || id[0] == 't') &&
            (id[1] == 'L' || id[1] == 'l')) {
            id = id.substr(2);
        }
        r.light = std::stoi(id);
        r.cars = std::stoi(cars_s);
    } catch (...) {
        r.light = 0;
    }
    return r;
}

inline void add_record(HourTable& table, const Record& r) {
    if (r.hour.empty() || r.light <= 0) return;
    table[r.hour][r.light] += r.cars;
}

inline uint64_t checksum(const HourTable& table) {
    uint64_t s = 0;
    int hi = 1;
    for (const auto& [hour, lights] : table) {
        for (const auto& [id, cars] : lights) {
            s += static_cast<uint64_t>(hi) * static_cast<uint64_t>(id) *
                 static_cast<uint64_t>(cars);
        }
        ++hi;
    }
    return s;
}

inline void print_top_n(const HourTable& table, int top_n, int max_print, std::ostream& os) {
    int printed = 0;
    for (const auto& [hour, lights] : table) {
        if (max_print >= 0 && printed >= max_print) {
            os << "... (" << (static_cast<int>(table.size()) - printed)
               << " more hour(s) omitted; checksum still covers all hours)\n";
            break;
        }
        std::vector<std::pair<long long, int>> ranked;
        ranked.reserve(lights.size());
        for (const auto& [id, cars] : lights) ranked.push_back({cars, id});
        std::sort(ranked.begin(), ranked.end(), [](const auto& a, const auto& b) {
            if (a.first != b.first) return a.first > b.first;
            return a.second < b.second;
        });

        int n = top_n;
        if (n < 1 || n > static_cast<int>(ranked.size())) n = static_cast<int>(ranked.size());

        std::string hh;
        if (hour.size() >= 13) {
            hh = hour.substr(11, 2);
        } else if (hour.size() == 4) {
            const int hod = std::stoi(hour) % 24;
            char buf[8];
            std::snprintf(buf, sizeof(buf), "%02d", hod);
            hh = buf;
        } else {
            hh = hour;
        }
        os << "Hour " << hh << ":00-" << hh << ":59  top " << n
           << " congested lights\n";
        os << "  Rank  Light  Cars\n";
        for (int i = 0; i < n; ++i) {
            os << "  " << (i + 1) << "     " << ranked[i].second << "      "
               << ranked[i].first << "\n";
        }
        ++printed;
    }
}

inline const char* flag_cstr(int argc, char** argv, const std::string& name) {
    for (int i = 1; i < argc - 1; ++i) {
        if (std::string(argv[i]) == name) return argv[i + 1];
    }
    return nullptr;
}

inline std::string default_report_name(const std::string& input, const std::string& tag) {
    std::string base = input;
    const auto slash = base.find_last_of("/\\");
    if (slash != std::string::npos) base = base.substr(slash + 1);
    const auto dot = base.rfind('.');
    if (dot != std::string::npos && dot > 0) base = base.substr(0, dot);
    return base + tag + ".txt";
}

inline void print_and_save(const std::string& text, const std::string& outfile) {
    std::cout << text;
    std::ofstream out(outfile);
    if (!out) die("Could not write " + outfile);
    out << text;
    std::cout << "Wrote output to: " << outfile << "\n";
}

inline int flag_int(int argc, char** argv, const std::string& name, int fallback) {
    for (int i = 1; i < argc - 1; ++i) {
        if (std::string(argv[i]) == name) return std::stoi(argv[i + 1]);
    }
    return fallback;
}
