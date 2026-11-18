package examples.interceptors

import cats.effect.{IO, IOApp, Resource}
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
import org.http4s.{Header, Headers, HttpApp}
import org.http4s.client.Client
import org.http4s.implicits.*
import me.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sClientBuilder, ConnectHttp4sRouteBuilder}
import me.ivovk.connect_rpc_scala.opentelemetry.OpenTelemetryServerInterceptor
import me.ivovk.connect_rpc_scala.service.*
import org.typelevel.ci.CIString

object Main extends IOApp.Simple {

  private val requestIdHeader = CIString("x-request-id")

  private val sendRequestId = UnaryInterceptor.transformContext[IO, String, ConnectRequestContext] { id =>
    IO.pure(ConnectRequestContext(headers = Headers(Header.Raw(requestIdHeader, id))))
  }

  private object Echo extends EchoServiceConnect.Service[IO, ConnectRequestContext] {
    def echo(request: EchoRequest, context: ConnectRequestContext): IO[ConnectResponse[EchoResponse]] =
      if request.message.isBlank then
        IO.raiseError(ConnectException(Code.InvalidArgument, "The message is empty"))
      else if request.simulateUnavailable then
        IO.raiseError(ConnectException(Code.Unavailable, "Simulated service outage"))
      else {
        val requestId = context.headers.get(requestIdHeader).map(_.head.value).getOrElse("")
        IO.pure(ConnectResponse(EchoResponse(request.message, requestId)))
      }
  }

  private final class Dependencies(using AllocatorIO) {
    private lazy val meterProvider: SdkMeterProvider = provide:
      Resource.fromAutoCloseable(IO {
        val reader = PeriodicMetricReader.builder(ConsoleMetrics).build()
        SdkMeterProvider.builder().registerMetricReader(reader).build()
      })

    private lazy val app: HttpApp[IO] = provide:
      val metrics = OpenTelemetryServerInterceptor.builder
        .meter(meterProvider.get("interceptors-example"))
        .build[IO, ConnectRequestContext]()
      val logging = ErrorLoggingInterceptor.builder[IO].build[ConnectRequestContext]()
      val service = EchoServiceConnect.bind(Echo, metrics.andThen(logging))
      ConnectHttp4sRouteBuilder.forService(service).build

    lazy val client: EchoServiceConnect.Service[IO, String] = {
      val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
      EchoServiceConnect.Client[IO](transport).withInterceptor(sendRequestId)
    }
  }

  def run: IO[Unit] = Allocator.builder[IO].create(Dependencies()).use { dependencies =>
    val client = dependencies.client
    for {
      response <- client.echo(EchoRequest("hello"), "request-1")
      _ <- IO.println(s"Echoed '${response.message.message}' with request ID ${response.message.requestId}")
      _ <- showFailure(client.echo(EchoRequest(), "request-2"))
      _ <- showFailure(client.echo(EchoRequest("hello", simulateUnavailable = true), "request-3"))
    } yield ()
  }

  private def showFailure(call: IO[ConnectResponse[EchoResponse]]): IO[Unit] =
    call.attempt.flatMap {
      case Left(error: ConnectException) =>
        IO.println(s"Client received ${error.code.name.stripPrefix("CODE_")}")
      case Left(error) => IO.raiseError(error)
      case Right(_)    => IO.raiseError(new IllegalStateException("Expected the call to fail"))
    }

  private object ConsoleMetrics extends MetricExporter {
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
