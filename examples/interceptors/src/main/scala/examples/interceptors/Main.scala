package examples.interceptors

import cats.effect.{IO, IOApp, Resource, SyncIO}
import connectrpc.Code
import examples.interceptors.echo.{EchoRequest, EchoResponse, EchoServiceConnect}
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.{InstrumentType, SdkMeterProvider}
import io.opentelemetry.sdk.metrics.data.{
  AggregationTemporality,
  HistogramPointData,
  LongPointData,
  MetricData,
}
import io.opentelemetry.sdk.metrics.`export`.{MetricExporter, PeriodicMetricReader}
import me.ivovk.cedi.syntax.*
import org.http4s.{Header, HttpApp}
import org.http4s.client.Client
import org.http4s.implicits.*
import me.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sClientBuilder, ConnectHttp4sRouteBuilder}
import me.ivovk.connect_rpc_scala.interceptors.{
  ErrorLoggingInterceptor,
  OpenTelemetryServerInterceptor,
  UnaryInterceptor,
}
import me.ivovk.connect_rpc_scala.*
import org.typelevel.ci.CIString
import org.typelevel.vault.Key

object Main extends IOApp.Simple {

  val requestIdHeader = CIString("x-request-id")

  case class RequestId(value: String)

  val requestIdKey: Key[RequestId] = Key.newKey[SyncIO, RequestId].unsafeRunSync()
  given AttributeReader[RequestId] = AttributeReader.fromKey(requestIdKey)

  val sendRequestId: UnaryInterceptor[IO] = UnaryInterceptor.transformContext[IO] { context =>
    IO.pure(context.attribute[RequestId].fold(context) { id =>
      context.copy(headers = context.headers.put(Header.Raw(requestIdHeader, id.value)))
    })
  }

  val readRequestId = UnaryInterceptor.transformContext[IO] { context =>
    IO.pure(context.headers.get(requestIdHeader).fold(context) { values =>
      context.withAttribute(requestIdKey, RequestId(values.head.value))
    })
  }

  object Echo extends EchoServiceConnect.Service[IO] {
    def echo(request: EchoRequest, context: ConnectRequestContext): IO[ConnectResponse[EchoResponse]] =
      if request.message.isBlank then
        IO.raiseError(ConnectException(Code.InvalidArgument, "The message is empty"))
      else if request.simulateUnavailable then
        IO.raiseError(ConnectException(Code.Unavailable, "Simulated service outage"))
      else {
        val requestId = context.attribute[RequestId].fold("")(_.value)
        IO.pure(ConnectResponse(EchoResponse(request.message, requestId)))
      }
  }

  class Dependencies(using AllocatorIO) {
    lazy val meterProvider: SdkMeterProvider = provide:
      Resource.fromAutoCloseable(IO {
        val reader = PeriodicMetricReader.builder(ConsoleMetrics).build()
        SdkMeterProvider.builder().registerMetricReader(reader).build()
      })

    lazy val app: HttpApp[IO] = provide:
      val metrics = OpenTelemetryServerInterceptor.builder
        .meter(meterProvider.get("interceptors-example"))
        .build[IO]()
      val logging = ErrorLoggingInterceptor.builder[IO].build()
      val service = EchoServiceConnect.bind(Echo, UnaryInterceptor.chain(metrics, logging, readRequestId))
      ConnectHttp4sRouteBuilder.forService(service).build

    lazy val client: EchoServiceConnect.Service[IO] = {
      val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
      EchoServiceConnect.Client[IO](transport).withInterceptor(sendRequestId)
    }
  }

  def run: IO[Unit] = Allocator.builder[IO].create(Dependencies()).use { dependencies =>
    val client = dependencies.client
    for {
      response <- client.echo(EchoRequest("hello"), requestContext("request-1"))
      _ <- IO.println(s"Echoed '${response.message.message}' with request ID ${response.message.requestId}")
      _ <- showFailure(client.echo(EchoRequest(), requestContext("request-2")))
      _ <- showFailure(
        client.echo(EchoRequest("hello", simulateUnavailable = true), requestContext("request-3"))
      )
    } yield ()
  }

  def requestContext(id: String): ConnectRequestContext =
    ConnectRequestContext().withAttribute(requestIdKey, RequestId(id))

  def showFailure(call: IO[ConnectResponse[EchoResponse]]): IO[Unit] =
    call.attempt.flatMap {
      case Left(error: ConnectException) =>
        IO.println(s"Client received ${error.code.name.stripPrefix("CODE_")}")
      case Left(error) => IO.raiseError(error)
      case Right(_)    => IO.raiseError(new IllegalStateException("Expected the call to fail"))
    }

  object ConsoleMetrics extends MetricExporter {
    override def `export`(metrics: java.util.Collection[MetricData]): CompletableResultCode = {
      metrics.forEach { metric =>
        metric.getData.getPoints.forEach {
          case point: LongPointData =>
            println(s"${metric.getName} ${point.getAttributes} count=${point.getValue}")
          case point: HistogramPointData =>
            println(s"${metric.getName} ${point.getAttributes} count=${point.getCount} sum=${point.getSum} s")
          case _ => ()
        }
      }
      CompletableResultCode.ofSuccess()
    }

    override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
      AggregationTemporality.CUMULATIVE

    override def flush(): CompletableResultCode    = CompletableResultCode.ofSuccess()
    override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
  }
}
