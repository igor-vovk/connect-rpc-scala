package me.ivovk.connect_rpc_scala.benchmark

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.google.protobuf.ByteString
import connectrpc.{Code, Error, ErrorDetailsAny}
import fs2.Stream
import org.http4s.ContentCoding
import me.ivovk.connect_rpc_scala.http.codec.{
  EncodeOptions,
  EntityToDecode,
  JsonMessageCodec,
  JsonSerdesBuilder,
}
import me.ivovk.connect_rpc_scala.service.ConnectHeaders
import org.openjdk.jmh.annotations.{
  Benchmark,
  BenchmarkMode,
  Fork,
  Measurement,
  Mode,
  OutputTimeUnit,
  Param,
  Scope,
  Setup,
  State,
  Warmup,
}

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
@State(Scope.Benchmark)
class JsonMessageCodecBenchmark {

  @Param(Array("1", "10", "100"))
  var detailsCount: Int = 0

  private given runtime: IORuntime = IORuntime.global

  private var codec: JsonMessageCodec[IO] = scala.compiletime.uninitialized
  private var message: Error              = scala.compiletime.uninitialized
  private var encoded: EntityToDecode[IO] = scala.compiletime.uninitialized

  private val encodeOptions =
    EncodeOptions(StandardCharsets.UTF_8, ContentCoding.identity)

  @Setup
  def setup(): Unit = {
    codec = JsonSerdesBuilder[IO]().build.codec
    message = Error(
      code = Code.Internal,
      message = Some("A representative Connect error response"),
      details = Vector.tabulate(detailsCount) { index =>
        ErrorDetailsAny(
          `type` = s"type.googleapis.com/benchmark.Payload$index",
          value = ByteString.copyFrom(Array.tabulate(128)(i => (index + i).toByte)),
        )
      },
    )

    val bytes = serializeBody()
    encoded = EntityToDecode(Stream.emits(bytes), ConnectHeaders.empty)
  }

  @Benchmark
  def parseBody(): Error =
    codec.decode[Error](encoded).compile.lastOrError.unsafeRunSync()

  @Benchmark
  def serializeBody(): Array[Byte] =
    codec
      .encode(Stream.emit(message), encodeOptions)
      .body
      .compile
      .to(Array)
      .unsafeRunSync()
}
