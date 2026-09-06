// data_gen.cpp
// Generates a synthetic input data file for the Traffic Control Simulator
// (SIT315 Task M2.T3D).
//
// Each line of output represents one traffic signal reading:
//   timestamp,light_id,cars_passed
//
// - timestamp is minutes-since-start, incrementing by 5 (12 readings/hour,
//   one every 5 minutes, as required by the brief).
// - light_id identifies which of the X traffic signals the reading is from
//   (formatted TL01, TL02, ... so it sorts and parses cleanly).
// - cars_passed is the number of cars that passed that light in that
//   5-minute window. A simple day/night + rush-hour profile is layered on
//   top of random noise so the "top-N congested light per hour" output
//   is actually meaningful to look at, rather than pure random noise.
//
// Usage:
//   ./data_gen <num_lights> <num_hours> <seed> <output_file>
//
// Example:
//   ./data_gen 20 168 42 traffic_data_medium.csv
//   (20 traffic lights, 7 days of data, reproducible with seed 42)

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

int main(int argc, char** argv) {
    if (argc != 5) {
        std::fprintf(stderr,
            "Usage: %s <num_lights> <num_hours> <seed> <output_file>\n",
            argv[0]);
        return 1;
    }

    int num_lights = std::atoi(argv[1]);
    int num_hours  = std::atoi(argv[2]);
    unsigned seed  = static_cast<unsigned>(std::atoi(argv[3]));
    const char* out_path = argv[4];

    if (num_lights <= 0 || num_hours <= 0) {
        std::fprintf(stderr, "num_lights and num_hours must be positive\n");
        return 1;
    }

    std::FILE* f = std::fopen(out_path, "w");
    if (!f) {
        std::fprintf(stderr, "Could not open %s for writing\n", out_path);
        return 1;
    }

    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> baseline(2, 15);   // quiet-hour cars
    std::uniform_int_distribution<int> rushNoise(0, 20);  // extra rush noise

    const int readings_per_hour = 12; // every 5 minutes
    long timestamp = 0;               // minutes since simulation start

    for (int h = 0; h < num_hours; ++h) {
        int hour_of_day = h % 24;
        // Simple rush-hour profile: busier 7-9am and 4-6pm, quiet overnight.
        bool is_rush = (hour_of_day >= 7 && hour_of_day <= 9) ||
                       (hour_of_day >= 16 && hour_of_day <= 18);
        bool is_overnight = (hour_of_day >= 0 && hour_of_day <= 4);

        for (int r = 0; r < readings_per_hour; ++r) {
            for (int light = 1; light <= num_lights; ++light) {
                int cars = baseline(rng);
                if (is_rush) {
                    cars += 25 + rushNoise(rng);
                } else if (is_overnight) {
                    cars = std::max(0, cars - 5);
                }
                // Give a couple of lights a consistently higher load so
                // "most congested" output has a clear, checkable answer.
                if (light == 1 || light == num_lights) {
                    cars += 10;
                }
                std::fprintf(f, "%ld,TL%02d,%d\n", timestamp, light, cars);
            }
            timestamp += 5;
        }
    }

    std::fclose(f);
    long total_lines = static_cast<long>(num_hours) * readings_per_hour * num_lights;
    std::fprintf(stderr, "Wrote %ld lines to %s (%d lights, %d hours)\n",
                 total_lines, out_path, num_lights, num_hours);
    return 0;
}
