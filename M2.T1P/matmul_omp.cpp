//Parallel matrix multiplication using OpenMP: C = A x B
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <omp.h>

using namespace std;
using namespace std::chrono;

// Same seed as the sequential / std::thread programs so A and B match for a given N
const unsigned int RNG_SEED = 42;

// Fill an N x N matrix with random integers in the range 0–99
void fillRandom(double* matrix, int n)
{
    for (int i = 0; i < n * n; i++)
    {
        matrix[i] = static_cast<double>(rand() % 100);
    }
}

// C = A x B. The outer loop is the parallel region.
// Static schedule = contiguous row blocks, matching the std::thread split.
void multiply(const double* A, const double* B, double* C, int n)
{
    // Fork a team of threads. Each iteration i is a row of C assigned to one thread.
    // default(shared): A, B, C, n are shared; i / j / k / sum are private per iteration.
#pragma omp parallel for schedule(static)
    for (int i = 0; i < n; i++)          // rows of C — split across threads
    {
        for (int j = 0; j < n; j++)      // columns of C — stays inside the thread
        {
            double sum = 0.0;
            for (int k = 0; k < n; k++)  // dot product of A row i and B column j
            {
                sum += A[i * n + k] * B[k * n + j];
            }
            C[i * n + j] = sum;
        }
    }
    // Implicit barrier: all threads finish their rows before multiply() returns
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
    out << "implementation OpenMP\n";
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

    // Ask OpenMP to use this many threads in the next parallel region
    omp_set_num_threads(numThreads);

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

    // Time only the OpenMP multiply (includes fork / work / implicit join)
    auto start = high_resolution_clock::now();
    multiply(A, B, C, n);
    auto stop = high_resolution_clock::now();

    const double ms = duration_cast<duration<double, milli>>(stop - start).count();
    const double chk = checksum(C, n);

    // Print results
    cout.precision(6);
    cout << fixed;
    cout << "OpenMP Result:" << endl;
    cout << "N: " << n << endl;
    cout << "Threads: " << omp_get_max_threads() << endl;
    cout << "Time: " << ms << " ms" << endl;
    cout.precision(17);
    cout << "Checksum: " << chk << endl;

    // Assignment requires writing the output matrix to a file (not timed)
    writeOutput("omp_output.txt", C, n, numThreads, chk, ms);
    cout << "Output written to: omp_output.txt" << endl;

    // Clean up allocated memory
    free(A);
    free(B);
    free(C);
    return 0;
}
