#include "traffic_common.hpp"

#include <mpi.h>

#include <climits>
#include <cstring>
#include <iomanip>

// One (hour, light) total as it travels between ranks.
struct KV {
    char hour[16];
    std::int32_t light;
    std::int64_t cars;
};

static MPI_Datatype kv_type;
static const int TAG_SHUFFLE = 10;

enum class Part { Hour, Pair };

static void fail(const std::string& msg) {
    std::cerr << msg << "\n";
    MPI_Abort(MPI_COMM_WORLD, 1);
}

static bool has_flag(int argc, char** argv, const std::string& name) {
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == name) return true;
    }
    return false;
}

static KV make_kv(const std::string& hour, int light, long long cars) {
    KV kv{};
    if (hour.size() >= sizeof(kv.hour)) fail("Hour key too long: " + hour);
    std::memcpy(kv.hour, hour.data(), hour.size());
    kv.light = light;
    kv.cars = cars;
    return kv;
}

// Ranks on different machines must agree on a key's owner, so this is a fixed
// hash (FNV-1a) rather than std::hash.
static std::uint32_t fnv1a(const void* p, size_t n, std::uint32_t h = 2166136261u) {
    const unsigned char* b = static_cast<const unsigned char*>(p);
    for (size_t i = 0; i < n; ++i) {
        h ^= b[i];
        h *= 16777619u;
    }
    return h;
}

// hour: every light of an hour goes to one rank (coarse keys, can be uneven).
// pair: each (hour, light) is placed on its own, so one busy hour is spread out.
static int owner(const std::string& hour, int light, Part part, int nranks) {
    std::uint32_t h = fnv1a(hour.data(), hour.size());
    if (part == Part::Pair) h = fnv1a(&light, sizeof(light), h);
    return static_cast<int>(h % static_cast<std::uint32_t>(nranks));
}

// Rank 0 cuts the file into one byte range per rank. As in traffic_pc, a
// header line is skipped. Each cut is then moved forward to the next line
// start, so every range holds whole lines and can be read in one go.
static std::vector<std::uint64_t> line_cuts(const std::string& path, int parts) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in) fail("Could not open " + path);
    const std::uint64_t file_end = static_cast<std::uint64_t>(in.tellg());
    in.seekg(0);
    std::string first;
    std::getline(in, first);
    std::uint64_t data_begin = 0;
    if (parse_line(first).light <= 0) {
        const auto pos = in.tellg();
        data_begin = pos < 0 ? file_end : static_cast<std::uint64_t>(pos);
    }
    if (data_begin >= file_end) fail("File has no data rows: " + path);

    std::vector<std::uint64_t> cuts(static_cast<size_t>(parts) + 1);
    cuts[0] = data_begin;
    cuts[static_cast<size_t>(parts)] = file_end;
    const std::uint64_t span = file_end - data_begin;
    for (int i = 1; i < parts; ++i) {
        std::uint64_t pos = data_begin + span * static_cast<std::uint64_t>(i) / parts;
        if (pos < cuts[static_cast<size_t>(i) - 1]) pos = cuts[static_cast<size_t>(i) - 1];
        if (pos > data_begin && pos < file_end) {
            in.clear();
            in.seekg(static_cast<std::streamoff>(pos - 1));
            char c = 0;
            while (in.get(c) && c != '\n') {
            }
            pos = in ? static_cast<std::uint64_t>(in.tellg()) : file_end;
        }
        cuts[static_cast<size_t>(i)] = pos;
    }
    return cuts;
}

static std::string read_range(const std::string& path, std::uint64_t a, std::uint64_t b) {
    std::string buf(static_cast<size_t>(b - a), '\0');
    std::ifstream in(path, std::ios::binary);
    if (!in) fail("Could not open " + path);
    in.seekg(static_cast<std::streamoff>(a));
    in.read(&buf[0], static_cast<std::streamsize>(buf.size()));
    if (static_cast<std::uint64_t>(in.gcount()) != b - a) fail("Short read on " + path);
    return buf;
}

// --input scatter: only rank 0 touches the file and sends each rank its bytes.
static std::string scatter_ranges(const std::string& path, const std::vector<std::uint64_t>& cuts,
                                  int rank, int nranks) {
    std::vector<int> counts(static_cast<size_t>(nranks)), displs(static_cast<size_t>(nranks));
    std::string whole;
    if (rank == 0) {
        if (cuts.back() > static_cast<std::uint64_t>(INT_MAX)) {
            fail("--input scatter needs a file under 2 GB (MPI int counts). Use --input shared.");
        }
        whole = read_range(path, 0, cuts.back());
        for (int r = 0; r < nranks; ++r) {
            counts[static_cast<size_t>(r)] = static_cast<int>(cuts[r + 1] - cuts[r]);
            displs[static_cast<size_t>(r)] = static_cast<int>(cuts[r]);
        }
    }
    int mine = 0;
    MPI_Scatter(counts.data(), 1, MPI_INT, &mine, 1, MPI_INT, 0, MPI_COMM_WORLD);
    std::string buf(static_cast<size_t>(mine), '\0');
    MPI_Scatterv(whole.data(), counts.data(), displs.data(), MPI_CHAR, &buf[0], mine, MPI_CHAR, 0,
                 MPI_COMM_WORLD);
    return buf;
}

// Map. With the combiner on, rows are summed per (hour, light) in local first.
// With it off, every row becomes its own message entry.
static long long map_chunk(const std::string& buf, bool combine, Part part, int nranks,
                           HourTable& local, std::vector<std::vector<KV>>& out) {
    long long n = 0;
    size_t pos = 0;
    while (pos < buf.size()) {
        size_t nl = buf.find('\n', pos);
        if (nl == std::string::npos) nl = buf.size();
        Record r = parse_line(buf.substr(pos, nl - pos));
        pos = nl + 1;
        if (r.light <= 0) continue;
        ++n;
        if (combine) {
            add_record(local, r);
        } else {
            out[static_cast<size_t>(owner(r.hour, r.light, part, nranks))].push_back(
                make_kv(r.hour, r.light, r.cars));
        }
    }
    if (combine) {
        for (const auto& [hour, lights] : local) {
            for (const auto& [id, cars] : lights) {
                out[static_cast<size_t>(owner(hour, id, part, nranks))].push_back(
                    make_kv(hour, id, cars));
            }
        }
    }
    return n;
}

static void add_kv(HourTable& table, const KV& kv) { table[kv.hour][kv.light] += kv.cars; }

// Shuffle. Bucket r goes to rank r, and what arrives is added to owned.
// Returns the MPI_Wtime at which all data had arrived.
static double shuffle(const std::vector<std::vector<KV>>& out, bool p2p, int rank, int nranks,
                      HourTable& owned) {
    const size_t P = static_cast<size_t>(nranks);
    std::vector<int> sendc(P), recvc(P), sdisp(P), rdisp(P);
    for (size_t r = 0; r < P; ++r) sendc[r] = static_cast<int>(out[r].size());
    MPI_Alltoall(sendc.data(), 1, MPI_INT, recvc.data(), 1, MPI_INT, MPI_COMM_WORLD);
    int nsend = 0, nrecv = 0;
    for (size_t r = 0; r < P; ++r) {
        sdisp[r] = nsend;
        rdisp[r] = nrecv;
        nsend += sendc[r];
        nrecv += recvc[r];
    }
    std::vector<KV> recv(static_cast<size_t>(nrecv));
    const size_t me = static_cast<size_t>(rank);

    if (!p2p) {
        std::vector<KV> send;
        send.reserve(static_cast<size_t>(nsend));
        for (const auto& b : out) send.insert(send.end(), b.begin(), b.end());
        MPI_Alltoallv(send.data(), sendc.data(), sdisp.data(), kv_type, recv.data(), recvc.data(),
                      rdisp.data(), kv_type, MPI_COMM_WORLD);
        const double arrived = MPI_Wtime();
        for (const KV& kv : recv) add_kv(owned, kv);
        return arrived;
    }

    // Non-blocking: post every receive and send, then reduce this rank's own
    // bucket while the messages are in flight.
    std::vector<MPI_Request> reqs;
    reqs.reserve(2 * P);
    for (size_t r = 0; r < P; ++r) {
        if (r == me || recvc[r] == 0) continue;
        reqs.emplace_back();
        MPI_Irecv(recv.data() + rdisp[r], recvc[r], kv_type, static_cast<int>(r), TAG_SHUFFLE,
                  MPI_COMM_WORLD, &reqs.back());
    }
    for (size_t r = 0; r < P; ++r) {
        if (r == me || sendc[r] == 0) continue;
        reqs.emplace_back();
        MPI_Isend(out[r].data(), sendc[r], kv_type, static_cast<int>(r), TAG_SHUFFLE,
                  MPI_COMM_WORLD, &reqs.back());
    }
    for (const KV& kv : out[me]) add_kv(owned, kv);
    MPI_Waitall(static_cast<int>(reqs.size()), reqs.data(), MPI_STATUSES_IGNORE);
    const double arrived = MPI_Wtime();
    for (size_t r = 0; r < P; ++r) {
        if (r == me) continue;
        for (int i = 0; i < recvc[r]; ++i) add_kv(owned, recv[static_cast<size_t>(rdisp[r] + i)]);
    }
    return arrived;
}

// Reduce. Each owned (hour, light) total is complete on this rank, so the
// global top N of an hour is always inside the union of every rank's local
// top N. sums carries sum(light * cars) per hour for the checksum.
static void local_top_n(const HourTable& owned, int top_n, std::vector<KV>& cands,
                        std::vector<KV>& sums) {
    for (const auto& [hour, lights] : owned) {
        std::vector<std::pair<long long, int>> ranked;
        ranked.reserve(lights.size());
        std::uint64_t s = 0;
        for (const auto& [id, cars] : lights) {
            ranked.push_back({cars, id});
            s += static_cast<std::uint64_t>(id) * static_cast<std::uint64_t>(cars);
        }
        size_t n = ranked.size();
        if (top_n >= 1 && static_cast<size_t>(top_n) < n) n = static_cast<size_t>(top_n);
        std::partial_sort(ranked.begin(), ranked.begin() + static_cast<std::ptrdiff_t>(n),
                          ranked.end(), [](const auto& a, const auto& b) {
                              if (a.first != b.first) return a.first > b.first;
                              return a.second < b.second;
                          });
        for (size_t i = 0; i < n; ++i) cands.push_back(make_kv(hour, ranked[i].second, ranked[i].first));
        sums.push_back(make_kv(hour, 0, static_cast<long long>(s)));
    }
}

static std::vector<KV> gather_kv(const std::vector<KV>& mine, int rank, int nranks) {
    int n = static_cast<int>(mine.size());
    std::vector<int> counts(static_cast<size_t>(nranks)), displs(static_cast<size_t>(nranks));
    MPI_Gather(&n, 1, MPI_INT, counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);
    std::vector<KV> all;
    if (rank == 0) {
        int total = 0;
        for (int r = 0; r < nranks; ++r) {
            displs[static_cast<size_t>(r)] = total;
            total += counts[static_cast<size_t>(r)];
        }
        all.resize(static_cast<size_t>(total));
    }
    MPI_Gatherv(mine.data(), n, kv_type, all.data(), counts.data(), displs.data(), kv_type, 0,
                MPI_COMM_WORLD);
    return all;
}

// Per-rank numbers sent to rank 0 for the report.
enum Stat { S_BYTES, S_RECORDS, S_SENT, S_OWNED, S_MAP, S_SHUFFLE, S_REDUCE, S_COUNT };

static double imbalance(const std::vector<double>& st, int nranks, int col) {
    double mx = 0, sum = 0;
    for (int r = 0; r < nranks; ++r) {
        const double v = st[static_cast<size_t>(r * S_COUNT + col)];
        mx = std::max(mx, v);
        sum += v;
    }
    return sum > 0 ? mx / (sum / nranks) : 1.0;
}

static double slowest(const std::vector<double>& st, int nranks, int col) {
    double mx = 0;
    for (int r = 0; r < nranks; ++r) mx = std::max(mx, st[static_cast<size_t>(r * S_COUNT + col)]);
    return mx;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0, nranks = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);
    MPI_Type_contiguous(static_cast<int>(sizeof(KV)), MPI_BYTE, &kv_type);
    MPI_Type_commit(&kv_type);

    const std::string usage =
        "Usage: traffic_mpi <file> [topN] [--part pair|hour] [--input shared|scatter]\n"
        "                   [--exchange alltoallv|p2p] [--no-combine] [--max-print K] [--out file]";
    const char* part_s = flag_cstr(argc, argv, "--part");
    const char* input_s = flag_cstr(argc, argv, "--input");
    const char* exch_s = flag_cstr(argc, argv, "--exchange");
    const std::string part_name = part_s ? part_s : "pair";
    const std::string input_mode = input_s ? input_s : "shared";
    const std::string exchange = exch_s ? exch_s : "alltoallv";
    if (argc < 2 || (part_name != "pair" && part_name != "hour") ||
        (input_mode != "shared" && input_mode != "scatter") ||
        (exchange != "alltoallv" && exchange != "p2p")) {
        if (rank == 0) std::cerr << usage << "\n";
        MPI_Finalize();
        return 1;
    }
    const std::string path = argv[1];
    const int top_n = (argc >= 3 && argv[2][0] != '-') ? std::stoi(argv[2]) : 5;
    const int max_print = flag_int(argc, argv, "--max-print", -1);
    const Part part = part_name == "pair" ? Part::Pair : Part::Hour;
    const bool combine = !has_flag(argc, argv, "--no-combine");
    const bool p2p = exchange == "p2p";
    const char* out_flag = flag_cstr(argc, argv, "--out");
    const std::string outfile =
        out_flag ? out_flag
                 : default_report_name(path, "_mpi_p" + std::to_string(nranks) + "_" + part_name);

    MPI_Barrier(MPI_COMM_WORLD);
    const double t0 = MPI_Wtime();

    // Split: rank 0 picks the line-aligned cuts and every rank learns them.
    std::vector<std::uint64_t> cuts(static_cast<size_t>(nranks) + 1);
    if (rank == 0) cuts = line_cuts(path, nranks);
    MPI_Bcast(cuts.data(), nranks + 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    std::string chunk = input_mode == "scatter" ? scatter_ranges(path, cuts, rank, nranks)
                                                : read_range(path, cuts[rank], cuts[rank + 1]);
    HourTable local;
    std::vector<std::vector<KV>> out(static_cast<size_t>(nranks));
    const long long nrec = map_chunk(chunk, combine, part, nranks, local, out);
    long long sent = 0;
    for (const auto& b : out) sent += static_cast<long long>(b.size());
    const double bytes = static_cast<double>(chunk.size());
    std::string().swap(chunk);
    HourTable().swap(local);
    const double t1 = MPI_Wtime();

    HourTable owned;
    const double t2 = shuffle(out, p2p, rank, nranks, owned);
    std::vector<std::vector<KV>>().swap(out);

    std::vector<KV> cands, sums;
    local_top_n(owned, top_n, cands, sums);
    long long nowned = 0;
    for (const auto& [hour, lights] : owned) nowned += static_cast<long long>(lights.size());
    const double t3 = MPI_Wtime();

    const std::vector<KV> all_cands = gather_kv(cands, rank, nranks);
    const std::vector<KV> all_sums = gather_kv(sums, rank, nranks);

    double mine[S_COUNT] = {bytes, static_cast<double>(nrec), static_cast<double>(sent),
                            static_cast<double>(nowned), (t1 - t0) * 1e3, (t2 - t1) * 1e3,
                            (t3 - t2) * 1e3};
    std::vector<double> st(static_cast<size_t>(nranks * S_COUNT));
    MPI_Gather(mine, S_COUNT, MPI_DOUBLE, st.data(), S_COUNT, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    char name[MPI_MAX_PROCESSOR_NAME] = {};
    int name_len = 0;
    MPI_Get_processor_name(name, &name_len);
    std::vector<char> names(static_cast<size_t>(nranks) * MPI_MAX_PROCESSOR_NAME);
    MPI_Gather(name, MPI_MAX_PROCESSOR_NAME, MPI_CHAR, names.data(), MPI_MAX_PROCESSOR_NAME,
               MPI_CHAR, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        HourTable top;
        for (const KV& kv : all_cands) add_kv(top, kv);
        std::map<std::string, std::uint64_t> hour_sums;
        for (const KV& kv : all_sums) hour_sums[kv.hour] += static_cast<std::uint64_t>(kv.cars);
        std::uint64_t cs = 0, hi = 1;
        for (const auto& [hour, s] : hour_sums) cs += hi++ * s;
        const double t4 = MPI_Wtime();

        long long records = 0, keys = 0;
        for (int r = 0; r < nranks; ++r) {
            records += static_cast<long long>(st[static_cast<size_t>(r * S_COUNT + S_RECORDS)]);
            keys += static_cast<long long>(st[static_cast<size_t>(r * S_COUNT + S_OWNED)]);
        }
        std::vector<std::pair<std::string, int>> hosts;
        for (int r = 0; r < nranks; ++r) {
            const std::string h(&names[static_cast<size_t>(r) * MPI_MAX_PROCESSOR_NAME]);
            if (hosts.empty() || hosts.back().first != h) hosts.push_back({h, 0});
            ++hosts.back().second;
        }

        std::ostringstream report;
        report << std::fixed << std::setprecision(2);
        report << "Mode: mpi-mapreduce\n";
        report << "Input: " << path << "\n";
        report << "Processes: " << nranks << "  Hosts:";
        for (const auto& [h, n] : hosts) report << " " << h << "(" << n << ")";
        report << "\n";
        report << "Partition: " << part_name << "  Input: " << input_mode
               << "  Exchange: " << exchange << "  Combiner: " << (combine ? "on" : "off")
               << "  TopN: " << top_n << "\n";
        report << "Records: " << records << "  Hours: " << hour_sums.size()
               << "  Keys: " << keys << "\n";
        report << "Checksum: " << cs << "\n";
        report << "Time_ms: " << (t4 - t0) * 1e3 << "\n";
        report << "Phase_ms (slowest rank): read+map " << slowest(st, nranks, S_MAP)
               << "  shuffle " << slowest(st, nranks, S_SHUFFLE) << "  reduce "
               << slowest(st, nranks, S_REDUCE) << "  gather " << (t4 - t3) * 1e3 << "\n";
        report << "  Rank  Bytes        Records     KeysSent    KeysOwned   Map_ms     Shuffle_ms "
                  "Reduce_ms\n";
        for (int r = 0; r < nranks; ++r) {
            const double* s = &st[static_cast<size_t>(r * S_COUNT)];
            report << "  " << std::left << std::setw(6) << r << std::setprecision(0)
                   << std::setw(13) << s[S_BYTES] << std::setw(12) << s[S_RECORDS]
                   << std::setw(12) << s[S_SENT] << std::setw(12) << s[S_OWNED]
                   << std::setprecision(2) << std::setw(11) << s[S_MAP] << std::setw(11)
                   << s[S_SHUFFLE] << s[S_REDUCE] << std::right << "\n";
        }
        report << "Imbalance (max/avg): records " << imbalance(st, nranks, S_RECORDS)
               << "  keys owned " << imbalance(st, nranks, S_OWNED) << "\n";
        report << std::defaultfloat;
        print_top_n(top, top_n, max_print, report);
        print_and_save(report.str(), outfile);
    }

    MPI_Type_free(&kv_type);
    MPI_Finalize();
    return 0;
}
