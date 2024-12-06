package org.ivovk.connect_rpc_scala.conformance

import cats.effect.Async
import cats.implicits.*
import com.google.protobuf.ByteString
import connectrpc.conformance.v1.*
import io.grpc.{Metadata, Status, StatusRuntimeException}
import org.ivovk.connect_rpc_scala.syntax.all.*

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*

case class UnaryHandlerResponse(payload: ConformancePayload, trailers: Metadata)

class ConformanceServiceImpl[F[_] : Async] extends ConformanceServiceFs2GrpcTrailers[F, Metadata] {

  override def unary(
    request: UnaryRequest,
    ctx: Metadata
  ): F[(UnaryResponse, Metadata)] = {
    for
      res <- handleUnaryRequest(
        request.getResponseDefinition,
        Seq(request.toProtoAny),
        ctx
      )
    yield (
      UnaryResponse(res.payload.some),
      res.trailers
    )
  }

  override def idempotentUnary(
    request: IdempotentUnaryRequest,
    ctx: Metadata,
  ): F[(IdempotentUnaryResponse, Metadata)] = {
    for
      res <- handleUnaryRequest(
        request.getResponseDefinition,
        Seq(request.toProtoAny),
        ctx
      )
    yield (
      IdempotentUnaryResponse(res.payload.some),
      res.trailers
    )
  }

  private def handleUnaryRequest(
    responseDefinition: UnaryResponseDefinition,
    requests: Seq[com.google.protobuf.any.Any],
    ctx: Metadata,
  ): F[UnaryHandlerResponse] = {
    val requestInfo = ConformancePayload.RequestInfo(
      requestHeaders = mkConformanceHeaders(ctx),
      timeoutMs = extractTimeout(ctx),
      requests = requests
    )

    val trailers = mkMetadata(Seq.concat(
      responseDefinition.responseHeaders,
      responseDefinition.responseTrailers.map(h => h.copy(name = s"trailer-${h.name}")),
    ))

    val responseData = responseDefinition.response match {
      case UnaryResponseDefinition.Response.ResponseData(bs) =>
        bs.some
      case UnaryResponseDefinition.Response.Empty =>
        none
      case UnaryResponseDefinition.Response.Error(Error(code, message, _)) =>
        val status = Status.fromCodeValue(code.value).withDescription(message.orNull)

        throw new StatusRuntimeException(status, trailers).withDetails(requestInfo)
    }

    val sleep = Duration(responseDefinition.responseDelayMs, TimeUnit.MILLISECONDS)

    Async[F].delayBy(
      UnaryHandlerResponse(
        ConformancePayload(
          responseData.getOrElse(ByteString.EMPTY),
          requestInfo.some
        ),
        trailers
      ).pure[F],
      sleep
    )

  }

  private def keyof(key: String): Metadata.Key[String] =
    Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)

  private def mkConformanceHeaders(metadata: Metadata): Seq[Header] = {
    metadata.keys().asScala.map { key =>
      Header(key, metadata.getAll(keyof(key)).asScala.toSeq)
    }.toSeq
  }

  private def mkMetadata(headers: Seq[Header]): Metadata = {
    val metadata = new Metadata()
    headers.foreach { h =>
      h.value.foreach { v =>
        metadata.put(keyof(h.name), v)
      }
    }
    metadata
  }

  private def extractTimeout(metadata: Metadata): Option[Long] = {
    Option(metadata.get(keyof("grpc-timeout")))
      .map(v => v.substring(0, v.length - 1).toLong / 1000)
  }

  override def serverStream(
    request: ServerStreamRequest,
    ctx: Metadata
  ): fs2.Stream[F, ServerStreamResponse] = ???

  override def clientStream(
    request: fs2.Stream[F, ClientStreamRequest],
    ctx: Metadata
  ): F[(ClientStreamResponse, Metadata)] = ???

  override def bidiStream(
    request: fs2.Stream[F, BidiStreamRequest],
    ctx: Metadata
  ): fs2.Stream[F, BidiStreamResponse] = ???

  // This endpoint must stay unimplemented
  override def unimplemented(
    request: UnimplementedRequest,
    ctx: Metadata
  ): F[(UnimplementedResponse, Metadata)] = ???

}
