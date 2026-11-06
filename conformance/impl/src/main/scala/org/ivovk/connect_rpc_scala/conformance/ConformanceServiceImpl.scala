package org.ivovk.connect_rpc_scala.conformance

import cats.effect.Async
import cats.implicits.*
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import connectrpc.conformance.v1.*
import connectrpc.conformance.v1.UnaryResponseDefinition.Response
import org.ivovk.connect_rpc_scala.conformance.util.ConformanceHeadersConv
import org.ivovk.connect_rpc_scala.service.*
import scalapb.GeneratedMessage

import scala.concurrent.duration.*

private case class UnaryHandlerResponse(
  payload: ConformancePayload,
  headers: ConnectHeaders,
  trailers: ConnectHeaders,
)

class ConformanceServiceImpl[F[_]: Async] extends ConformanceServiceConnect.Service[F] {

  override def unary(
    request: UnaryRequest,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[UnaryResponse]] =
    handleUnary(
      request.getResponseDefinition,
      request,
      ctx,
    ).map { result =>
      ConnectResponse(UnaryResponse(result.payload.some), result.headers, result.trailers)
    }

  override def idempotentUnary(
    request: IdempotentUnaryRequest,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[IdempotentUnaryResponse]] =
    handleUnary(
      request.getResponseDefinition,
      request,
      ctx,
    ).map { result =>
      ConnectResponse(IdempotentUnaryResponse(result.payload.some), result.headers, result.trailers)
    }

  override def unimplemented(
    request: UnimplementedRequest,
    _ctx: ConnectRequestContext,
  ): F[ConnectResponse[UnimplementedResponse]] =
    ConnectException(
      connectrpc.Code.Unimplemented,
      s"Method ${request.companion.scalaDescriptor.fullName} is not implemented",
    ).raiseError

  private def handleUnary[A <: GeneratedMessage](
    responseDefinition: UnaryResponseDefinition,
    request: A,
    ctx: ConnectRequestContext,
  ): F[UnaryHandlerResponse] = {
    val requestInfo = ConformancePayload.RequestInfo(
      requestHeaders = ConformanceHeadersConv.toHeaderSeq(ctx.headers),
      timeoutMs = ctx.timeout.map(_.toMillis),
      requests = Seq(Any.pack(request)),
    )

    val headers  = ConformanceHeadersConv.toConnectHeaders(responseDefinition.responseHeaders)
    val trailers = ConformanceHeadersConv.toConnectHeaders(
      responseDefinition.responseTrailers.map(header => header.copy(name = s"trailer-${header.name}"))
    )

    responseDefinition.response match {
      case Response.ResponseData(byteString) =>
        delayedResponse(responseDefinition, requestInfo, headers, trailers, byteString)
      case Response.Empty =>
        delayedResponse(responseDefinition, requestInfo, headers, trailers, ByteString.EMPTY)
      case Response.Error(error) =>
        val detail = connectrpc.ErrorDetailsAny(
          `type` = requestInfo.companion.scalaDescriptor.fullName,
          value = requestInfo.toByteString,
        )
        ConnectException(
          code = connectrpc.Code.fromValue(error.code.value),
          message = error.getMessage,
          details = Seq(detail),
          headers = headers,
          trailers = trailers,
        ).raiseError
    }
  }

  private def delayedResponse(
    responseDefinition: UnaryResponseDefinition,
    requestInfo: ConformancePayload.RequestInfo,
    headers: ConnectHeaders,
    trailers: ConnectHeaders,
    responseData: ByteString,
  ): F[UnaryHandlerResponse] =
    Async[F].sleep(responseDefinition.responseDelayMs.millis) *>
      UnaryHandlerResponse(
        ConformancePayload(responseData, requestInfo.some),
        headers,
        trailers,
      ).pure[F]
}
