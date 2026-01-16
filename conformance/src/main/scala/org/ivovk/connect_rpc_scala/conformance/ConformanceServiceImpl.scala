package org.ivovk.connect_rpc_scala.conformance

import cats.effect.Async
import cats.implicits.*
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import connectrpc.conformance.v1.*
import connectrpc.conformance.v1.UnaryResponseDefinition.Response
import fs2.Stream
import io.grpc.internal.GrpcUtil
import io.grpc.{Metadata, Status, StatusRuntimeException}
import org.ivovk.connect_rpc_scala.conformance.util.ConformanceHeadersConv.*
import org.ivovk.connect_rpc_scala.syntax.all.*
import scalapb.GeneratedMessage

import scala.concurrent.duration.*

case class UnaryHandlerResponse(payload: ConformancePayload, trailers: Metadata)

class ConformanceServiceImpl[F[_]]()(using F: Async[F])
    extends ConformanceServiceFs2GrpcTrailers[F, Metadata] {

  override def unary(
    request: UnaryRequest,
    ctx: Metadata,
  ): F[(UnaryResponse, Metadata)] =
    for res <- handleUnaryAndClientStreaming(
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
    for res <- handleUnaryAndClientStreaming(
        request.getResponseDefinition,
        Seq(request),
        ctx,
      )
    yield (
      IdempotentUnaryResponse(res.payload.some),
      res.trailers,
    )

  override def clientStream(
    request: Stream[F, ClientStreamRequest],
    ctx: Metadata,
  ): F[(ClientStreamResponse, Metadata)] =
    for
      requests <- request.compile.to(Seq)
      resp     <- handleUnaryAndClientStreaming(
        requests.flatMap(_.responseDefinition).head,
        requests,
        ctx,
      )
    yield (
      ClientStreamResponse(resp.payload.some),
      resp.trailers,
    )

  private def handleUnaryAndClientStreaming(
    responseDefinition: UnaryResponseDefinition,
    requests: Seq[GeneratedMessage],
    ctx: Metadata,
  ): F[UnaryHandlerResponse] = {
    val requestInfo = ConformancePayload.RequestInfo(
      requestHeaders = toHeaderSeq(ctx),
      timeoutMs = extractTimeoutMs(ctx),
      requests = requests.map(Any.pack),
    )

    val trailers = toMetadata(
      Seq.concat(
        responseDefinition.responseHeaders,
        responseDefinition.responseTrailers.map(_.trailerize),
      )
    )

    val responseData = responseDefinition.response match {
      case Response.ResponseData(byteString) => byteString
      case Response.Empty                    => ByteString.EMPTY
      case Response.Error(error)             =>
        throw statusRuntimeExceptionFromError(error, trailers).withDetails(requestInfo)
    }

    F.sleep(responseDefinition.responseDelayMs.millis) *>
      UnaryHandlerResponse(
        ConformancePayload(
          responseData,
          requestInfo.some,
        ),
        trailers,
      ).pure[F]
  }

  private def extractTimeoutMs(metadata: Metadata): Option[Long] =
    Option(metadata.get(GrpcUtil.TIMEOUT_KEY)).map(_ / 1_000_000)

  override def serverStream(
    request: ServerStreamRequest,
    ctx: Metadata,
  ): Stream[F, ServerStreamResponse] = {
    val responseDefinition = request.getResponseDefinition

    val requestInfo = ConformancePayload.RequestInfo(
      requestHeaders = toHeaderSeq(ctx),
      timeoutMs = extractTimeoutMs(ctx),
      requests = Seq(Any.pack(request)),
    )

//    val headers  = toMetadata(responseDefinition.responseHeaders)
//    val trailers = toMetadata(responseDefinition.responseTrailers.map(_.trailerize))

    val bodyStream =
      Stream.emits(responseDefinition.responseData)
        .zip(Stream(Some(requestInfo)) ++ Stream.constant(None)) // Only first frame gets requestInfo
        .evalMap { (data, requestInfo) =>
          F.sleep(responseDefinition.responseDelayMs.millis) *>
            ServerStreamResponse(ConformancePayload(data, requestInfo).some).pure[F]
        }

    val error = Stream.fromOption(responseDefinition.error).flatMap { err =>
      Stream.raiseError(statusRuntimeExceptionFromError(err).withDetails(requestInfo))
    }

    bodyStream ++ error
  }

  override def bidiStream(
    request: Stream[F, BidiStreamRequest],
    ctx: Metadata,
  ): Stream[F, BidiStreamResponse] = ???

  // This endpoint must stay unimplemented
  override def unimplemented(
    request: UnimplementedRequest,
    ctx: Metadata,
  ): F[(UnimplementedResponse, Metadata)] = ???

  private def statusRuntimeExceptionFromError(
    error: Error,
    trailers: Metadata = Metadata(),
  ): StatusRuntimeException = {
    val status = Status.fromCodeValue(error.code.value)
      .withDescription(error.message.orNull)

    StatusRuntimeException(status, trailers)
  }

}
