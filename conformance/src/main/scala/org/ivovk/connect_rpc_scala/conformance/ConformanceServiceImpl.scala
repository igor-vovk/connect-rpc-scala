package org.ivovk.connect_rpc_scala.conformance

import cats.effect.Async
import cats.implicits.*
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import connectrpc.conformance.v1.*
import io.grpc.internal.GrpcUtil
import io.grpc.{Metadata, Status}
import org.ivovk.connect_rpc_scala.conformance.util.ConformanceHeadersConv
import org.ivovk.connect_rpc_scala.syntax.all.*
import scalapb.GeneratedMessage

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

case class UnaryHandlerResponse(payload: ConformancePayload, trailers: Metadata)

class ConformanceServiceImpl[F[_]: Async] extends ConformanceServiceFs2GrpcTrailers[F, Metadata] {

  override def unary(
    request: UnaryRequest,
    ctx: Metadata,
  ): F[(UnaryResponse, Metadata)] =
    for res <- handleUnaryRequest(
        request.getResponseDefinition,
        Seq(request),
        ctx,
      )
    yield (
      UnaryResponse(res.payload.some),
      res.trailers,
    )

  override def idempotentUnary(
    request: IdempotentUnaryRequest,
    ctx: Metadata,
  ): F[(IdempotentUnaryResponse, Metadata)] =
    for res <- handleUnaryRequest(
        request.getResponseDefinition,
        Seq(request),
        ctx,
      )
    yield (
      IdempotentUnaryResponse(res.payload.some),
      res.trailers,
    )

  private def handleUnaryRequest(
    responseDefinition: UnaryResponseDefinition,
    requests: Seq[GeneratedMessage],
    ctx: Metadata,
  ): F[UnaryHandlerResponse] = {
    val requestInfo = ConformancePayload.RequestInfo(
      requestHeaders = ConformanceHeadersConv.toHeaderSeq(ctx),
      timeoutMs = extractTimeoutMs(ctx),
      requests = requests.map(Any.pack),
    )

    val trailers = ConformanceHeadersConv.toMetadata(
      Seq.concat(
        responseDefinition.responseHeaders,
        responseDefinition.responseTrailers.map(h => h.copy(name = s"trailer-${h.name}")),
      )
    )

    val responseData = responseDefinition.response match {
      case UnaryResponseDefinition.Response.ResponseData(byteString) =>
        byteString
      case UnaryResponseDefinition.Response.Empty =>
        ByteString.EMPTY
      case UnaryResponseDefinition.Response.Error(Error(code, message, _)) =>
        throw Status.fromCodeValue(code.value)
          .withDescription(message.orNull)
          .asRuntimeException(trailers)
          .withDetails(requestInfo)
    }

    val sleep = Duration(responseDefinition.responseDelayMs, TimeUnit.MILLISECONDS)

    Async[F].delayBy(
      UnaryHandlerResponse(
        ConformancePayload(
          responseData,
          requestInfo.some,
        ),
        trailers,
      ).pure[F],
      sleep,
    )

  }

  private def extractTimeoutMs(metadata: Metadata): Option[Long] =
    Option(metadata.get(GrpcUtil.TIMEOUT_KEY)).map(_ / 1_000_000)

  override def serverStream(
    request: ServerStreamRequest,
    ctx: Metadata,
  ): fs2.Stream[F, ServerStreamResponse] = ???

  override def clientStream(
    request: fs2.Stream[F, ClientStreamRequest],
    ctx: Metadata,
  ): F[(ClientStreamResponse, Metadata)] = ???

  override def bidiStream(
    request: fs2.Stream[F, BidiStreamRequest],
    ctx: Metadata,
  ): fs2.Stream[F, BidiStreamResponse] = ???

  // This endpoint must stay unimplemented
  override def unimplemented(
    request: UnimplementedRequest,
    ctx: Metadata,
  ): F[(UnimplementedResponse, Metadata)] = ???

}
