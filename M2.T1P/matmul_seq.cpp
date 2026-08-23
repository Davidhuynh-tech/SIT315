//Sequential matrix multiplication: C = A x B
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>

using namespace std;
using namespace std::chrono;

// Fixed seed so sequential / thread / OpenMP runs of the same N produce
// identical A and B, which lets us compare checksums for correctness.
const unsigned int RNG_SEED = 42;

// Fill an N x N matrix with random integers in the range 0–99
void fillRandom(double* matrix, int n)
{
    for (int i = 0; i < n * n; i++)
    {
        matrix[i] = static_cast<double>(rand() % 100);
    }
}

// Sequential C = A x B using three nested loops.
// To get C[i,j] (row i, column j): multiply every element in row i of A
// by every element in column j of B and add the products.
void multiply(const double* A, const double* B, double* C, int n)
{
    for (int i = 0; i < n; i++)          // each row of C
    {
        for (int j = 0; j < n; j++)      // each column of C
        {
            double sum = 0.0;
            for (int k = 0; k < n; k++)  // dot product of A row i and B column j
            {
                sum += A[i * n + k] * B[k * n + j];
            }
            C[i * n + j] = sum;
        }
    }
}

// Sum of every element in C — used to check that parallel results match this program
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
void writeOutput(const char* path, const double* C, int n, double chk, double ms)
{
    ofstream out(path);
    if (!out)
    {
        cerr << "Failed to write " << path << endl;
        return;
    }
    out.precision(17);
    out << "N " << n << "\n";
    out << "threads 1\n";
    out << "implementation sequential\n";
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
    if (argc < 2)
    {
        cerr << "Usage: " << argv[0] << " N" << endl;
        return 1;
    }

    const int n = atoi(argv[1]);
    if (n <= 0)
    {
        cerr << "N must be a positive integer" << endl;
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

    // Initialise inputs (not included in the timed section)
    fillRandom(A, n);
    fillRandom(B, n);

    // Time only the matrix multiplication
    auto start = high_resolution_clock::now();
    multiply(A, B, C, n);
    auto stop = high_resolution_clock::now();

    const double ms = duration_cast<duration<double, milli>>(stop - start).count();
    const double chk = checksum(C, n);

    // Print results
    cout.precision(6);
    cout << fixed;
    cout << "Sequential Result:" << endl;
    cout << "N: " << n << endl;
    cout << "Threads: 1" << endl;
    cout << "Time: " << ms << " ms" << endl;
    cout.precision(17);
    cout << "Checksum: " << chk << endl;

    // Assignment requires writing the output matrix to a file (not timed)
    writeOutput("seq_output.txt", C, n, chk, ms);
    cout << "Output written to: seq_output.txt" << endl;

    // Clean up allocated memory
    free(A);
    free(B);
    free(C);
    return 0;
}
