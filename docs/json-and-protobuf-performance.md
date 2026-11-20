# JSON and Protobuf Performance

For these unary echo RPCs, binary protobuf sent 14–46% fewer HTTP bytes and allocated 5–29% less
heap per round trip. Mean RPC latency was 10–15% lower with protobuf in this run, with overlapping
confidence intervals at every payload size. The tables show JSON → protobuf.

## Codec operations

Each operation encodes or decodes one unary Connect body in memory. The timings include the
library codec, Cats Effect, and HTTP entity handling. The traffic column gives the uncompressed
size of one body. [MessageCodecBenchmark](../benchmarks/src/main/scala/me/ivovk/connect_rpc_scala/benchmark/MessageCodecBenchmark.scala)

| Operation | Payload | Latency (µs/op) | Body bytes | Heap allocation (B/op) |
| --- | --- | ---: | ---: | ---: |
| Encode | Small | 21.1 ± 0.5 → 20.4 ± 0.2 | 46 → 12 | 2,923 → 2,071 |
| Encode | Medium | 22.2 ± 0.5 → 20.7 ± 0.6 | 228 → 82 | 5,138 → 2,130 |
| Encode | Large | 24.1 ± 0.6 → 20.5 ± 0.6 | 588 → 228 | 11,432 → 2,228 |
| Decode | Small | 28.6 ± 0.9 → 26.2 ± 1.2 | 46 → 12 | 10,480 → 8,932 |
| Decode | Medium | 34.7 ± 2.1 → 26.7 ± 1.1 | 228 → 82 | 16,272 → 9,324 |
| Decode | Large | 41.7 ± 1.2 → 27.8 ± 0.6 | 588 → 228 | 29,368 → 10,020 |

## Client-server round trips

Each operation sends one unary echo RPC over a reused loopback HTTP/1.1 connection. The client and
server both run in the same JVM, so the timings and allocations include HTTP handling and encoding
and decoding at both ends. HTTP traffic counts both start lines, headers, and bodies at the HTTP
layer. [ClientServerBenchmark](../benchmarks/src/main/scala/me/ivovk/connect_rpc_scala/benchmark/ClientServerBenchmark.scala)

| Payload | Latency (µs/RPC) | Body bytes/RPC | HTTP bytes/RPC | Heap allocation (KiB/RPC) |
| --- | ---: | ---: | ---: | ---: |
| Small | 403.5 ± 65.3 → 363.1 ± 59.5 | 92 → 24 | 478 → 412 | 161.3 → 153.6 |
| Medium | 418.2 ± 58.8 → 377.7 ± 50.7 | 456 → 164 | 845 → 553 | 179.1 → 155.5 |
| Large | 450.6 ± 60.6 → 381.2 ± 38.8 | 1,176 → 456 | 1,564 → 846 | 223.6 → 159.7 |

## Measurement setup

Both benchmarks use the same echo messages with 3, 12, and 30 populated fields. The run used
JMH 1.37 and OpenJDK 27.0.1.1 on a Debian 13 x86-64 VM with eight vCPUs on an AMD Ryzen 7
5700G. Each case used one benchmark thread, two JVM forks, five one-second warmup iterations,
and five one-second measurement iterations per fork. The forked JVMs used G1 and a fixed 512 MiB heap.
[Echo payloads](../benchmarks/src/main/scala/me/ivovk/connect_rpc_scala/benchmark/EchoPayloads.scala)

Latency is JMH's mean time per operation. The value after ± is the half-width of the 99.9%
confidence interval. Allocations are estimated heap bytes allocated per operation across the JVM,
reported as `gc.alloc.rate.norm`.
[JMH allocation profiler](https://github.com/openjdk/jmh/blob/1.37/jmh-core/src/main/java/org/openjdk/jmh/profile/GCProfiler.java)

```shell
sbt 'benchmarks/Jmh/run -wi 5 -i 5 -w 1s -r 1s -f 2 -t 1 -foe true -prof gc -jvmArgsAppend "-Xms512m -Xmx512m -XX:+UseG1GC" .*(MessageCodecBenchmark|ClientServerBenchmark).*'
```

Traffic was counted in a separate run by tracing TCP socket writes with Linux `strace`.
For each RPC, the HTTP `Content-Length` values identify the uncompressed body bytes. The sum
of the request and response writes gives the HTTP bytes per call. Each payload and codec
combination produced identical byte counts across four exchanges.

```shell
for codec in json protobuf; do
  strace -ff -ttt -yy -xx -s 4096 -e trace=write,writev,sendto,sendmsg \
    -o "/tmp/connect-http-$codec" \
    sbt "benchmarks/Jmh/run -bm ss -wi 0 -i 1 -f 0 -t 1 -p codec=$codec .*ClientServerBenchmark.*"
done
```
