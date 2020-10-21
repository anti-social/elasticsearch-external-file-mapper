Benchmark results
=================

| Benchmark                                 | Entries   | Result (million ops/s) |
|-------------------------------------------|-----------|-------------------------|
| SimpleHashMapBenchmark.benchmark_1_reader |   1_000_000 | 5.521 ± 0.859           |
| SimpleHashMapBenchmark.benchmark_1_reader |  10_000_000 | 6.285 ± 0.569           |
| SimpleHashMapBenchmark.benchmark_1_reader |  20_000_000 | 7.303 ± 0.098           |
| TroveHashMapBenchmark.benchmark_1_reader  |   1_000_000 | 14.255 ± 0.230          |
| TroveHashMapBenchmark.benchmark_1_reader  |  10_000_000 | 11.500 ± 0.335          |
| TroveHashMapBenchmark.benchmark_1_reader  |  20_000_000 | 11.827 ± 0.261          |