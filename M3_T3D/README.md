# M3.T3D — MPI MapReduce Traffic Control Simulator

The M2.T3D producer–consumer traffic simulator, rebuilt as an MPI MapReduce program and run on the SIT315 virtual machine. It reads `timestamp,light,cars` rows and prints the top N most congested lights for every hour.

| File | Role |
| --- | --- |
| `traffic_mpi.cpp` | MPI MapReduce program (the submission) |
| `traffic_common.hpp` | Parse, hour table and top-N printing, unchanged from M2.T3D |
| `traffic_seq.cpp` | Sequential baseline, unchanged from M2.T3D |
| `data_gen.cpp` | M2.T3D generator plus a `skew` option for uneven keys |
| `run_bench.sh` | Builds, checks correctness, times everything, runs the stress test |
| `DESIGN.md` | Solution design, results and analysis |
| `logs/` | Transcripts from the VM run (each starts with the exact command) |

The VM disk is non-persistent. Keep these files on Windows and copy them in each session.

## 1. Start the VM

Power on **SIT315-Head** in VMware Workstation and log in as `mpiuser` (password `SIT315`). Check the address with `hostname -I`. On the last run it was `192.168.57.132`.

## 2. Copy the folder in

From PowerShell in this folder:

```powershell
ssh mpiuser@192.168.57.132 "mkdir -p ~/M3_T3D"
scp traffic_mpi.cpp traffic_common.hpp traffic_seq.cpp data_gen.cpp Makefile run_bench.sh mpiuser@192.168.57.132:~/M3_T3D/
```

## 3. Compile

```bash
cd ~/M3_T3D
g++    -O2 -std=c++17 data_gen.cpp    -o data_gen
g++    -O2 -std=c++17 traffic_seq.cpp -o traffic_seq
mpicxx -O2 -std=c++17 traffic_mpi.cpp -o traffic_mpi
```

Or just `make`. The image has g++ 7.5 and MPICH 3.3 (Hydra `mpirun`, hostfile flag `-f`).

## 4. Generate data

```bash
./data_gen <lights> <hours> <seed> <out.csv> [skew]
```

| Command | Rows | Size |
| --- | ---: | ---: |
| `./data_gen 4 5 42 small.csv` | 240 | 2.7 KB |
| `./data_gen 80 720 42 xl.csv` | 691,200 | 9 MB |
| `./data_gen 100 5760 42 medium.csv` | 6,912,000 | 98 MB |
| `./data_gen 200 10000 42 large.csv` | 24,000,000 | 356 MB |
| `./data_gen 20000 48 42 skew.csv skew` | 1,488,000 | 21 MB |
| `./data_gen 40000 24 42 skew_day.csv skew` | 1,488,000 | 21 MB |

`skew`: only 08:00 and 17:00 have readings from every light. Other hours have 1 in 20 lights.

## 5. Run

```bash
mpirun -np <P> ./traffic_mpi <file> [topN] [--part pair|hour] [--input shared|scatter]
                             [--exchange alltoallv|p2p] [--no-combine] [--max-print K] [--out file]
```

Defaults: `--part pair --input shared --exchange alltoallv`, combiner on, top 5.

Small-file correctness demo (checksum must be **15810**, same as `traffic_seq`):

```bash
./traffic_seq small.csv 4
mpirun -np 2 ./traffic_mpi small.csv 4
mpirun -np 3 ./traffic_mpi small.csv 4 --part hour --exchange p2p
```

Large run:

```bash
./traffic_seq large.csv 5 --max-print 2
mpirun -np 4 ./traffic_mpi large.csv 5 --max-print 2
```

`--max-print K` still processes every hour. It only limits how many hours are printed. The checksum covers all of them.

Uneven keys:

```bash
mpirun -np 4 ./traffic_mpi skew.csv 5 --part hour --max-print 2
mpirun -np 4 ./traffic_mpi skew.csv 5 --part pair --max-print 2
```

Compare the `Imbalance ... keys owned` line and the per-rank `KeysOwned` column.

## 6. Full benchmark

```bash
bash run_bench.sh            # about 10 minutes plus up to 10 for the stress run
STRESS=0 bash run_bench.sh   # skip the stress run
```

Timings go to `logs/results.csv` (3 repeats of every size × process count). `check_summary.txt` lists the 16 small-file correctness checks.

