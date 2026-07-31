# Benchmarks

The benchmarks use [JMH](https://github.com/openjdk/jmh) to measure performance-sensitive paths through the public
library implementation.

Run the full benchmark configuration:

```shell
sbt "benchmarks/Jmh/run"
```

For a quicker local comparison:

```shell
sbt "benchmarks/Jmh/run -wi 3 -i 5 -f 1 -t 1"
```

Run comparisons on the same machine and JVM while other system load is low. Compare commits using identical JMH
arguments; absolute timings from different environments are not directly comparable.
