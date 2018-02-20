# Core Benchmarks
- Individual runtime results for all benchmarks is available in the **data.csv** file. Legend is as follows:
  - Spark Gen = Runtime of Cora generated Spark implementations
  - Spark Gen Speedup = Runtime speedup of Cora generated Spark implementations over sequential implementations
  - Manual = Runtime of manually written Spark implementations
  - Manual Speedup = Runtime speedup of manually written Spark implementations over sequential implementations

- All original benchmark implementations available under: **src/main/java/original**
- All Cora generated Spark implementations available under: **src/main/java/generated**
- All manual Spark implementations available under: **src/main/java/manual**
- All implementations for TPC-H benchmarks available under: **/src/main/java/tpch**
- All implementations for PageRank benchmark available under: **/src/main/java/pagerank**

- Query plans (captured from Spark UI) for Q15 and PageRank algorithm available under **/src/main/java/supplementary**
