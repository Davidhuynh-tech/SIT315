# SIT315 M2.T3D — Traffic Control Simulator

Bounded-buffer **producer–consumer** traffic simulator in C++. Producers read `{timestamp, light id, cars}` from a CSV. Consumers total cars per light per hour and print the **top-N** busiest lights. A sequential program is included as the baseline for speedup.

CSV data files are **not** in git. Generate them with `data_gen.cpp`.

## Files

| File | Role |
| --- | --- |
| `data_gen.cpp` | writes input CSVs |
| `traffic_common.hpp` | shared parse, hour table, print |
| `traffic_seq.cpp` | sequential (one thread) |
| `traffic_pc.cpp` | producer–consumer (threads) |

## Compile (MSYS2 MinGW 64-bit)

```bash
cd /c/Users/giaki/School/Sit_315/M2.T3D

g++ -O2 -std=c++17 data_gen.cpp -o data_gen
g++ -O2 -std=c++17 traffic_seq.cpp -o traffic_seq
g++ -O2 -std=c++17 traffic_pc.cpp -o traffic_pc
```

## Generate data

```bash
./data_gen <lights> <hours> <seed> <output.csv>
```

Each line is `minutes,TL01,cars`. Timestamp increases by 5 (12 samples per hour).

| Command | Rows |
| --- | ---: |
| `./data_gen 4 5 42 traffic_data_small.csv` | 240 |
| `./data_gen 20 48 42 medium.csv` | 11,520 |
| `./data_gen 40 168 42 large.csv` | 80,640 |
| `./data_gen 80 720 42 xl.csv` | 691,200 |

Small file: 4 lights × 5 hours × 12 samples = **240** rows.

## Run

Sequential:

```bash
./traffic_seq <file> [topN] [--max-print K] [--out file]
```

Producer–consumer:

```bash
./traffic_pc <file> <producers> <consumers> <buffer> [topN] [--max-print K] [--batch N] [--out file]
```

### Demo (correctness)

```bash
./traffic_seq traffic_data_small.csv 4
./traffic_pc  traffic_data_small.csv 2 2 8 4
```

Checksums must match (small file: **15810**).

`2 2 8 4` = 2 producers, 2 consumers, buffer 8, top 4 lights.

### Scaling / speedup

```bash
./traffic_seq xl.csv 5 --max-print 1
./traffic_pc  xl.csv 1 1 1 5 --max-print 1
./traffic_pc  xl.csv 8 8 128 5 --max-print 1
```

`--max-print 1` still processes the **whole** file; it only prints the first hour.

`--batch` is how many CSV rows sit in one buffer slot (default **256**). `--batch 1` locks almost every row (slow / DNF).

### DNF (meant not to finish)

Keep 8 producers / 8 consumers. Use a tiny effective buffer:

```bash
./data_gen 180 18000 42 dnf.csv
./traffic_pc dnf.csv 8 8 1 5 --batch 1 --max-print 1
```

Wait **10 minutes**, then Ctrl+C if `Time_ms` has not printed.

To DNF with buffer **128** (same as the fast scaling run), still use `--batch 1` and a much larger file:

```bash
./data_gen 250 50000 42 dnf.csv
./traffic_pc dnf.csv 8 8 128 5 --batch 1 --max-print 1
```

## Output files

When a run **finishes**, the same report is printed and saved (for example `traffic_data_small_seq.txt`, `xl_pc_8x8_buf128.txt`). Override with `--out myfile.txt`. A DNF run that is interrupted does not write a file.

## Design (short)

- **Buffer:** `std::queue` of batches, capacity = `buffer`. One **mutex**, two **condition variables** (`not_full_`, `not_empty_`).
- **Producers wait** when the buffer is full. **Consumers wait** when it is empty. This is **blocking** `wait`, not a spin loop.
- **Stop:** join producers → `close()` (`notify_all`) → join consumers.
- Each consumer has a **private** hour table; main merges after join. Sequential and parallel checksums must match.
