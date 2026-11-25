package me.ivovk.connect_rpc_scala.interceptors

import cats.effect.{Deferred, IO}
import cats.effect.unsafe.implicits.global
import connectrpc.Code
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.{InstrumentType, SdkMeterProvider}
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import io.opentelemetry.sdk.metrics.`export`.{CollectionRegistration, MetricReader}
import me.ivovk.connect_rpc_scala.{
  ConnectException,
  ConnectRequestContext,
  ConnectResponse,
  UnaryMethodDescriptor,
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*
import scala.util.Using

class OpenTelemetryServerInterceptorTest extends AnyFunSuite with Matchers {

  private val descriptor = UnaryMethodDescriptor(
    serviceName = "connectrpc.TestService",
    methodName = "Call",
    requestCompanion = connectrpc.Error,
    responseCompanion = connectrpc.Error,
  )

  private val request  = connectrpc.Error()
  private val context  = ConnectRequestContext()
  private val response = ConnectResponse(request)

  test("records a start before completion and a duration for each outcome") {
    val reader = new TestReader
    Using.resource(SdkMeterProvider.builder().registerMetricReader(reader).build()) { provider =>
      val interceptor = OpenTelemetryServerInterceptor.builder
        .meter(provider.get("test"))
        .build[IO]()
      val rejected = ConnectException(Code.PermissionDenied, "access denied")
      val failed   = IllegalStateException("unexpected failure")

      val program = for {
        entered <- Deferred[IO, Unit]
        fiber   <- interceptor.intercept[connectrpc.Error, connectrpc.Error](
          descriptor,
          request,
          context,
          _ => entered.complete(()) *> IO.never,
        ).start
        _      <- entered.get
        before <- IO(reader.metrics)
        _      <- fiber.cancel
        ok     <- interceptor.intercept(
          descriptor,
          request,
          context,
          ctx =>
            IO {
              ctx should be theSameInstanceAs context
              response
            },
        )
        denied <- interceptor.intercept[connectrpc.Error, connectrpc.Error](
          descriptor,
          request,
          context,
          _ => IO.raiseError(rejected),
        ).attempt
        internal <- interceptor.intercept[connectrpc.Error, connectrpc.Error](
          descriptor,
          request,
          context,
          _ => throw failed,
        ).attempt
      } yield (before, ok, denied, internal)

      val (before, ok, denied, internal) = program.unsafeRunSync()
      val startedName                    = "connectrpc.server.call.started"
      val durationName                   = "connectrpc.server.call.duration"

      ok shouldBe response
      denied shouldBe Left(rejected)
      internal shouldBe Left(failed)
      before.keySet shouldBe Set(startedName)
      before(startedName).getLongSumData.getPoints.asScala.map(_.getValue).toList shouldBe List(1L)

      val metrics = reader.metrics
      val started = metrics(startedName)
      started.getUnit shouldBe "{call}"
      val starts = started.getLongSumData.getPoints.asScala.toList
      starts.map(_.getValue) shouldBe List(4L)
      val method = Attributes.of(AttributeKey.stringKey("connectrpc.method"), "connectrpc.TestService/Call")
      starts.map(_.getAttributes) shouldBe List(method)

      val duration = metrics(durationName)
      duration.getUnit shouldBe "s"
      val points = duration.getHistogramData.getPoints.asScala.toList
      val status = AttributeKey.stringKey("connectrpc.status")
      points.map(_.getAttributes).toSet shouldBe Set("OK", "CANCELLED", "PERMISSION_DENIED", "INTERNAL")
        .map(code => method.toBuilder.put(status, code).build())
      all(points.map(_.getCount)) shouldBe 1L
      all(points.map(_.getBoundaries.asScala.head.doubleValue())) shouldBe 0.0
      all(points.map(_.getBoundaries.asScala.last.doubleValue())) shouldBe 100.0
    }
  }

  private final class TestReader extends MetricReader {
    private var registration = CollectionRegistration.noop()

    def metrics: Map[String, MetricData] =
      registration.collectAllMetrics().asScala.iterator.map(metric => metric.getName -> metric).toMap

    override def register(value: CollectionRegistration): Unit = registration = value

    override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
      AggregationTemporality.CUMULATIVE

    override def forceFlush(): CompletableResultCode = CompletableResultCode.ofSuccess()
    override def shutdown(): CompletableResultCode   = CompletableResultCode.ofSuccess()
  }
}
