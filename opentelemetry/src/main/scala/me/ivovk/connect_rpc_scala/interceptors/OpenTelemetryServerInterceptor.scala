package me.ivovk.connect_rpc_scala.interceptors

import cats.effect.{Outcome, Sync}
import cats.syntax.all.*
import connectrpc.Code
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.metrics.Meter
import me.ivovk.connect_rpc_scala.service.{ConnectException, ConnectResponse, UnaryMethodDescriptor}
import scalapb.GeneratedMessage

import scala.jdk.CollectionConverters.*

object OpenTelemetryServerInterceptor {

  private val InstrumentationScope = "connect-rpc-scala"
  private val DefaultMeter         = OpenTelemetry.noop().getMeter(InstrumentationScope)
  private val MethodKey            = AttributeKey.stringKey("connectrpc.method")
  private val StatusKey            = AttributeKey.stringKey("connectrpc.status")

  // The gRPC latency boundaries keep the histograms comparable across a transport migration.
  private val LatencyBuckets = List(
    0.0, 0.00001, 0.00005, 0.0001, 0.0003, 0.0006, 0.0008, 0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.008,
    0.01, 0.013, 0.016, 0.02, 0.025, 0.03, 0.04, 0.05, 0.065, 0.08, 0.1, 0.13, 0.16, 0.2, 0.25, 0.3, 0.4, 0.5,
    0.65, 0.8, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0,
  ).map(Double.box).asJava

  def builder: Builder = new Builder(DefaultMeter)

  final class Builder private[OpenTelemetryServerInterceptor] (selectedMeter: Meter) {

    private def copy(
      selectedMeter: Meter
    ): Builder =
      new Builder(selectedMeter)

    def sdk(openTelemetry: OpenTelemetry): Builder =
      copy(selectedMeter = openTelemetry.getMeter(InstrumentationScope))

    def meter(meter: Meter): Builder = copy(selectedMeter = meter)

    /** Builds an interceptor that records starts and outcomes for unary server calls. */
    def build[F[_]: Sync, Context](): OpenTelemetryServerInterceptor[F, Context] =
      new OpenTelemetryServerInterceptor(selectedMeter)
  }

  private def errorStatus(error: Throwable): String = error match {
    case exception: ConnectException =>
      exception.code match {
        case Code.Canceled                           => "CANCELLED"
        case Code.Unspecified | Code.Unrecognized(_) => "UNKNOWN"
        case code                                    => code.name.stripPrefix("CODE_")
      }
    case _ => "INTERNAL"
  }
}

final class OpenTelemetryServerInterceptor[F[_], Context] private (meter: Meter)(using F: Sync[F])
    extends UnaryInterceptor[F, Context, Context] {
  import OpenTelemetryServerInterceptor.*

  private val started = meter.counterBuilder("connectrpc.server.call.started")
    .setDescription("Number of Connect server calls started")
    .setUnit("{call}")
    .build()

  private val duration = meter.histogramBuilder("connectrpc.server.call.duration")
    .setDescription("Duration of a Connect unary server call at the service interceptor")
    .setUnit("s")
    .setExplicitBucketBoundariesAdvice(LatencyBuckets)
    .build()

  override def intercept[Request <: GeneratedMessage, Response <: GeneratedMessage](
    descriptor: UnaryMethodDescriptor[Request, Response],
    request: Request,
    context: Context,
    next: Context => F[ConnectResponse[Response]],
  ): F[ConnectResponse[Response]] = {
    val attributes = Attributes.of(MethodKey, s"${descriptor.serviceName}/${descriptor.methodName}")

    F.bracketCase(F.monotonic.flatTap(_ => F.delay(started.add(1, attributes)))) { _ =>
      F.defer(next(context))
    } { (start, outcome) =>
      val status = outcome match {
        case Outcome.Succeeded(_) => "OK"
        case Outcome.Canceled()   => "CANCELLED"
        case Outcome.Errored(err) => errorStatus(err)
      }

      F.monotonic.flatMap { end =>
        F.delay(
          duration.record(
            (end - start).toNanos / 1e9,
            attributes.toBuilder.put(StatusKey, status).build(),
          )
        )
      }
    }
  }
}
