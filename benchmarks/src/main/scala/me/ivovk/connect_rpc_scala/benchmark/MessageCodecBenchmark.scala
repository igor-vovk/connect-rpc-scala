package me.ivovk.connect_rpc_scala.benchmark

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import me.ivovk.connect_rpc_scala.http.codec.{
  EncodedEntity,
  EncodeOptions,
  EntityToDecode,
  JsonSerdesBuilder,
  MessageCodec,
  ProtoMessageCodec,
}
import org.http4s.ContentCoding
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
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
@State(Scope.Benchmark)
class MessageCodecBenchmark {

  @Param(Array("json", "protobuf"))
  var codec: String = scala.compiletime.uninitialized

  @Param(Array("small", "medium", "large"))
  var payloadSize: String = scala.compiletime.uninitialized

  private given runtime: IORuntime = IORuntime.global

  private var messageCodec: MessageCodec[IO]   = scala.compiletime.uninitialized
  private var message: GeneratedMessage        = scala.compiletime.uninitialized
  private var decodeBody: IO[GeneratedMessage] = scala.compiletime.uninitialized

  private val encodeOptions =
    EncodeOptions(StandardCharsets.UTF_8, ContentCoding.identity)

  @Setup
  def setup(): Unit = {
    messageCodec = codec match {
      case "json"     => JsonSerdesBuilder[IO]().build.codec
      case "protobuf" => new ProtoMessageCodec[IO]
      case other      => throw new IllegalArgumentException(s"Unknown codec: $other")
    }

    payloadSize match {
      case "small"  => prepare(EchoPayloads.small)
      case "medium" => prepare(EchoPayloads.medium)
      case "large"  => prepare(EchoPayloads.large)
      case other    => throw new IllegalArgumentException(s"Unknown payload size: $other")
    }
  }

  private def prepare[A <: GeneratedMessage](
    payload: A
  )(using companion: GeneratedMessageCompanion[A]): Unit = {
    message = payload
    val encoded = serializeBody()
    decodeBody = EntityToDecode(encoded.body, encoded.headers).decodeOne(companion, messageCodec)
    require(parseBody() == payload, s"The $payloadSize $codec payload failed to round-trip")
  }

  @Benchmark
  def parseBody(): GeneratedMessage =
    decodeBody.unsafeRunSync()

  @Benchmark
  def serializeBody(): EncodedEntity[IO] =
    messageCodec.encode(message, encodeOptions).unsafeRunSync()
}
