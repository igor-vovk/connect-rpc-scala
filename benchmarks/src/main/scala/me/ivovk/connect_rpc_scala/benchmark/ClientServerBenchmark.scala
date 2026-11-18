package me.ivovk.connect_rpc_scala.benchmark

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.comcast.ip4s.*
import fs2.io.net.SocketOption
import me.ivovk.connect_rpc_scala.benchmark.v1.echo.{
  EchoServiceConnect,
  LargePayload,
  MediumPayload,
  SmallPayload,
}
import me.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sClientBuilder, ConnectHttp4sRouteBuilder}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
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
  TearDown,
  Warmup,
}

import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
@State(Scope.Benchmark)
class ClientServerBenchmark {

  @Param(Array("json", "protobuf"))
  var codec: String = scala.compiletime.uninitialized

  private given runtime: IORuntime = IORuntime.global

  private var client: EchoServiceConnect.SimpleClient[IO] = scala.compiletime.uninitialized
  private var release: IO[Unit]                           = IO.unit

  private val small  = SmallPayload(sku = "WT-042", quantity = 120, inStock = true)
  private val medium = MediumPayload(
    sku = small.sku,
    quantity = small.quantity,
    inStock = small.inStock,
    name = "Insulated water bottle",
    category = "Outdoor gear",
    warehouse = "warehouse-west",
    bin = "A12",
    reserved = 16,
    incoming = 48,
    reorderPoint = 35,
    unitPriceCents = 2499,
    taxable = true,
  )
  private val large = LargePayload(
    sku = medium.sku,
    quantity = medium.quantity,
    inStock = medium.inStock,
    name = medium.name,
    category = medium.category,
    warehouse = medium.warehouse,
    bin = medium.bin,
    reserved = medium.reserved,
    incoming = medium.incoming,
    reorderPoint = medium.reorderPoint,
    unitPriceCents = medium.unitPriceCents,
    taxable = medium.taxable,
    supplier = "Summit Outdoor Supply",
    supplierSku = "SOS-750-BL",
    brand = "Summit",
    color = "Slate blue",
    material = "Stainless steel",
    countryOfOrigin = "CA",
    barcode = "0034567890123",
    lotNumber = "LOT-2026-042",
    aisle = "A",
    shelf = "12B",
    weightGrams = 325,
    widthMm = 75,
    heightMm = 260,
    depthMm = 75,
    unitsPerCase = 12,
    leadTimeDays = 14,
    trackLot = true,
    returnable = true,
  )

  private object Echo extends EchoServiceConnect.SimpleService[IO] {
    override def echoSmall(request: SmallPayload): IO[SmallPayload]    = IO.pure(request)
    override def echoMedium(request: MediumPayload): IO[MediumPayload] = IO.pure(request)
    override def echoLarge(request: LargePayload): IO[LargePayload]    = IO.pure(request)
  }

  @Setup
  def setup(): Unit = {
    val socketOptions = List(SocketOption.noDelay(true))
    val resources     = for {
      app    <- ConnectHttp4sRouteBuilder.forService(EchoServiceConnect.bind(Echo)).build
      server <- EmberServerBuilder.default[IO]
        .withHost(host"127.0.0.1")
        .withPort(port"0")
        .withAdditionalSocketOptions(socketOptions)
        .withHttpApp(app)
        .build
      httpClient <- EmberClientBuilder.default[IO].withAdditionalSocketOptions(socketOptions).build
      builder   = ConnectHttp4sClientBuilder(httpClient)
      transport = codec match {
        case "json"     => builder.build(server.baseUri)
        case "protobuf" => builder.enableBinaryFormat().build(server.baseUri)
        case other      => throw new IllegalArgumentException(s"Unknown codec: $other")
      }
      rpcClient = EchoServiceConnect.SimpleClient(transport)
      _ <- verifyEcho("small", small)(rpcClient.echoSmall).toResource
      _ <- verifyEcho("medium", medium)(rpcClient.echoMedium).toResource
      _ <- verifyEcho("large", large)(rpcClient.echoLarge).toResource
    } yield rpcClient

    val (rpcClient, close) = resources.allocated.unsafeRunSync()
    client = rpcClient
    release = close
  }

  private def verifyEcho[A](size: String, expected: A)(echo: A => IO[A]): IO[Unit] =
    echo(expected).flatMap { actual =>
      IO.raiseUnless(actual == expected)(
        new IllegalStateException(s"The $size $codec echo returned a different payload")
      )
    }

  @TearDown
  def tearDown(): Unit = release.unsafeRunSync()

  @Benchmark
  def smallBody(): SmallPayload = client.echoSmall(small).unsafeRunSync()

  @Benchmark
  def mediumBody(): MediumPayload = client.echoMedium(medium).unsafeRunSync()

  @Benchmark
  def largeBody(): LargePayload = client.echoLarge(large).unsafeRunSync()
}
