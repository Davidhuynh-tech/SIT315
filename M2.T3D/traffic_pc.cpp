// Producer-consumer Traffic Control Simulator 
#include "traffic_common.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <queue>
#include <thread>

// Bounded blocking buffer of batches.
using Batch = std::vector<Record>;
static int g_batch_size = 256;

class BoundedBuffer {
public:
    explicit BoundedBuffer(size_t cap) : cap_(cap < 1 ? 1 : cap) {}

    // Push a batch of records into the buffer. Wait if the buffer is full.
    void push(Batch rec) {
        std::unique_lock<std::mutex> lk(mu_);
        not_full_.wait(lk, [&] { return q_.size() < cap_ || closed_; });
        if (closed_) return;
        q_.push(std::move(rec));
        not_empty_.notify_one();
    }
    
    // Pop a batch of records from the buffer. Wait if the buffer is empty.
    bool pop(Batch& out) {
        std::unique_lock<std::mutex> lk(mu_);
        not_empty_.wait(lk, [&] { return !q_.empty() || closed_; });
        // If the buffer is empty and closed, return false to indicate no more data will be produced.
        if (q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop();
        not_full_.notify_one();
        return true;
    }
    // Close the buffer to signal that no more data will be produced.
    void close() {
        std::lock_guard<std::mutex> g(mu_);
        closed_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }
// Private members for the bounded buffer.
private:
    std::queue<Batch> q_;
    size_t cap_;
    bool closed_ = false;
    std::mutex mu_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
};
// Structure to hold file split information for producers.
struct FileSplit {
    std::uint64_t data_begin = 0;
    std::uint64_t file_end = 0;
    std::vector<std::pair<std::uint64_t, std::uint64_t>> ranges;
};
// Split the input file into ranges for each producer to process.
static FileSplit split_file(const std::string& path, int nprod) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in) die("Could not open " + path);
    FileSplit sp;
    sp.file_end = static_cast<std::uint64_t>(in.tellg());
    in.seekg(0);
    std::string first;
    std::getline(in, first);
    Record probe = parse_line(first);
    if (probe.light > 0) {
        sp.data_begin = 0;
    } else {
        sp.data_begin = static_cast<std::uint64_t>(in.tellg());
    }
    if (sp.data_begin >= sp.file_end) die("File has no data rows: " + path);

    if (nprod < 1) nprod = 1;
    const std::uint64_t span = sp.file_end - sp.data_begin;
    for (int i = 0; i < nprod; ++i) {
        std::uint64_t a = sp.data_begin + span * static_cast<std::uint64_t>(i) / nprod;
        std::uint64_t b = sp.data_begin + span * static_cast<std::uint64_t>(i + 1) / nprod;
        sp.ranges.push_back({a, b});
    }
    sp.ranges.back().second = sp.file_end;
    return sp;
}

static void produce_chunk(const std::string& path, std::uint64_t start, std::uint64_t end,
                          std::uint64_t data_begin, BoundedBuffer& buf) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return;
    if (start > data_begin) {
        in.seekg(static_cast<std::streamoff>(start - 1));
        char prev = 0;
        in.get(prev);
        if (prev != '\n') {
            std::string skip;
            std::getline(in, skip);
        }
    } else {
        in.seekg(static_cast<std::streamoff>(start));
    }

    Batch batch;
    batch.reserve(static_cast<size_t>(g_batch_size));
    auto flush = [&] {
        if (!batch.empty()) {
            buf.push(std::move(batch));
            batch.clear();
            batch.reserve(static_cast<size_t>(g_batch_size));
        }
    };

    while (in) {
        const auto pos = in.tellg();
        if (pos < 0) break;
        if (static_cast<std::uint64_t>(pos) >= end) break;
        std::string line;
        if (!std::getline(in, line)) break;
        Record r = parse_line(line);
        if (r.light > 0) {
            batch.push_back(std::move(r));
            if (static_cast<int>(batch.size()) >= g_batch_size) flush();
        }
    }
    flush();
}

// Each consumer thread runs this loop to pop batches from the buffer and process them.
static void consume_loop(BoundedBuffer& buf, HourTable& local, std::atomic<long long>& nrec) {
    Batch batch;
    while (buf.pop(batch)) {
        const long long before = nrec.fetch_add(static_cast<long long>(batch.size()),
                                                std::memory_order_relaxed);
        const long long after = before + static_cast<long long>(batch.size());
        for (const Record& r : batch) add_record(local, r);
        if (after / 2000000 != before / 2000000) {
            std::cerr << "pc: processed " << after << " rows\n";
        }
    }
}

// Main function for the producer-consumer traffic control simulator.
int main(int argc, char** argv) {
    if (argc < 5) {
        die("Usage: traffic_pc <file> <n_prod> <n_cons> <buf> [topN] [--max-print K] [--batch N] [--out file]");
    }
    const std::string path = argv[1];
    const int nprod = std::stoi(argv[2]);
    const int ncons = std::stoi(argv[3]);
    const int bufsz = std::stoi(argv[4]);
    const int top_n = (argc >= 6 && argv[5][0] != '-') ? std::stoi(argv[5]) : 5;
    const int max_print = flag_int(argc, argv, "--max-print", -1);
    g_batch_size = flag_int(argc, argv, "--batch", 256);
    if (g_batch_size < 1) g_batch_size = 1;
    if (nprod < 1 || ncons < 1 || bufsz < 1) die("n_prod, n_cons and buf must be >= 1");
    const char* out_flag = flag_cstr(argc, argv, "--out");
    std::string outfile;
    if (out_flag) {
        outfile = out_flag;
    } else {
        outfile = default_report_name(
            path, "_pc_" + std::to_string(nprod) + "x" + std::to_string(ncons) + "_buf" +
                      std::to_string(bufsz));
    }

    // Split the input file into ranges for each producer to process.
    FileSplit sp = split_file(path, nprod);
    BoundedBuffer buf(static_cast<size_t>(bufsz));
    std::vector<HourTable> locals(static_cast<size_t>(ncons));
    std::atomic<long long> nrec{0};
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    producers.reserve(static_cast<size_t>(nprod));
    consumers.reserve(static_cast<size_t>(ncons));

    // Start timing the producer-consumer processing
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < ncons; ++i) {
        consumers.emplace_back(consume_loop, std::ref(buf),
                               std::ref(locals[static_cast<size_t>(i)]), std::ref(nrec));
    }
    for (int i = 0; i < nprod; ++i) {
        producers.emplace_back(produce_chunk, path, sp.ranges[static_cast<size_t>(i)].first,
                               sp.ranges[static_cast<size_t>(i)].second, sp.data_begin,
                               std::ref(buf));
    }
    for (auto& th : producers) th.join();
    buf.close();
    for (auto& th : consumers) th.join();

    // Merge local tables from consumers into a single global table.
    HourTable table;
    for (const auto& local : locals) merge_hours(table, local);
    auto t1 = std::chrono::steady_clock::now();
    const double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    std::ostringstream report;
    report << "Mode: producer-consumer\n";
    report << "Input: " << path << "\n";
    report << "Producers: " << nprod << "  Consumers: " << ncons << "  Buffer: " << bufsz
           << "  Batch: " << g_batch_size << "  TopN: " << top_n << "\n";
    report << "Records: " << nrec.load() << "  Hours: " << table.size() << "\n";
    report << "Checksum: " << checksum(table) << "\n";
    report << "Time_ms: " << ms << "\n";
    print_top_n(table, top_n, max_print, report);
    print_and_save(report.str(), outfile);
    return 0;
}
