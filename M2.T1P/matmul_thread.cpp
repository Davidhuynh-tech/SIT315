//Parallel matrix multiplication using std::thread: C = A x B
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>

using namespace std;
using namespace std::chrono;

// Same seed as the sequential / OpenMP programs so A and B match for a given N
const unsigned int RNG_SEED = 42;

// Fill an N x N matrix with random integers in the range 0–99
void fillRandom(double* matrix, int n)
{
    for (int i = 0; i < n * n; i++)
    {
        matrix[i] = static_cast<double>(rand() % 100);
    }
}

// Worker function: one thread computes a contiguous block of rows of C.
// C[i,j] = sum over k of A[i,k] * B[k,j]  (same three nested loops as sequential)
void multiplyRows(const double* A, const double* B, double* C,
                  int n, int rowStart, int rowEnd)
{
    for (int i = rowStart; i < rowEnd; i++)  // only this thread's rows
    {
        for (int j = 0; j < n; j++)
        {
            double sum = 0.0;
            for (int k = 0; k < n; k++)
            {
                sum += A[i * n + k] * B[k * n + j];
            }
            C[i * n + j] = sum;
        }
    }
}

// Sum of every element in C — compared against the sequential checksum
double checksum(const double* matrix, int n)
{
    double sum = 0.0;
    for (int i = 0; i < n * n; i++)
    {
        sum += matrix[i];
    }
    return sum;
}

// Write result matrix C to a file (after timing has stopped)
void writeOutput(const char* path, const double* C, int n,
                 int threads, double chk, double ms)
{
    ofstream out(path);
    if (!out)
    {
        cerr << "Failed to write " << path << endl;
        return;
    }
    out.precision(17);
    out << "N " << n << "\n";
    out << "threads " << threads << "\n";
    out << "implementation std::thread\n";
    out << "time_ms " << ms << "\n";
    out << "checksum " << chk << "\n";
    for (int i = 0; i < n; i++)
    {
        for (int j = 0; j < n; j++)
        {
            out << C[i * n + j];
            if (j + 1 < n)
            {
                out << " ";
            }
        }
        out << "\n";
    }
}

int main(int argc, char* argv[])
{
    if (argc < 3)
    {
        cerr << "Usage: " << argv[0] << " N num_threads" << endl;
        return 1;
    }

    const int n = atoi(argv[1]);
    const int numThreads = atoi(argv[2]);
    if (n <= 0 || numThreads <= 0)
    {
        cerr << "N and num_threads must be positive integers" << endl;
        return 1;
    }

    srand(RNG_SEED);

    // Allocate three N x N matrices on the heap
    double* A = static_cast<double*>(malloc(static_cast<size_t>(n) * n * sizeof(double)));
    double* B = static_cast<double*>(malloc(static_cast<size_t>(n) * n * sizeof(double)));
    double* C = static_cast<double*>(malloc(static_cast<size_t>(n) * n * sizeof(double)));
    if (!A || !B || !C)
    {
        cerr << "Memory allocation failed for N=" << n << endl;
        free(A);
        free(B);
        free(C);
        return 1;
    }

    // Initialise inputs sequentially so the timed section is only the multiply
    fillRandom(A, n);
    fillRandom(B, n);

    // Partition size: how many rows each thread owns (last threads absorb remainder)
    const int rowsPerThread = n / numThreads;
    const int remainder = n % numThreads;

    // Time spawn + parallel work + join (the multiply kernel)
    auto start = high_resolution_clock::now();

    vector<thread> workers;
    workers.reserve(static_cast<size_t>(numThreads));

    int row = 0;
    for (int t = 0; t < numThreads; t++)
    {
        // First 'remainder' threads get one extra row when N is not divisible by T
        const int chunk = rowsPerThread + (t < remainder ? 1 : 0);
        const int rowStart = row;
        const int rowEnd = row + chunk;
        row = rowEnd;

        if (rowStart >= rowEnd)
        {
            // More threads than rows: extra threads do no work
            workers.emplace_back([]() {});
            continue;
        }

        // Launch a worker for this row block (disjoint write region of C)
        workers.emplace_back(multiplyRows, A, B, C, n, rowStart, rowEnd);
    }

    // Barrier: wait until every thread has finished its rows before using C
    for (auto& th : workers)
    {
        th.join();
    }

    auto stop = high_resolution_clock::now();

    const double ms = duration_cast<duration<double, milli>>(stop - start).count();
    const double chk = checksum(C, n);

    // Print results
    cout.precision(6);
    cout << fixed;
    cout << "std:thread Result:" << endl;
    cout << "N: " << n << endl;
    cout << "Threads: " << numThreads << endl;
    cout << "Rows per thread (approx): " << rowsPerThread << endl;
    cout << "Time: " << ms << " ms" << endl;
    cout.precision(17);
    cout << "Checksum: " << chk << endl;

    // Assignment requires writing the output matrix to a file (not timed)
    writeOutput("thread_output.txt", C, n, numThreads, chk, ms);
    cout << "Output written to: thread_output.txt" << endl;

    // Clean up allocated memory
    free(A);
    free(B);
    free(C);
    return 0;
}
