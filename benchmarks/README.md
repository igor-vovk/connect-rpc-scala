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

## Client-server round trips

`ClientServerBenchmark` measures the average time for one unary Connect RPC over loopback HTTP using an
http4s Ember client and server. Each request is echoed back, so the timing includes encoding and decoding on
both sides. Each codec uses the same request data, with 3 populated fields for `smallBody`, 12 for
`mediumBody`, and 30 for `largeBody`. The benchmark starts the server, creates the client, and checks all
three echo responses during trial setup. Timed requests reuse the client and prepared messages. Both ends
enable `TCP_NODELAY` so small writes are sent promptly.

Run all six JSON and protobuf cases with the default warmup, measurement, and fork settings.

```shell
sbt 'benchmarks/Jmh/run -t 1 .*ClientServerBenchmark.*'
```
