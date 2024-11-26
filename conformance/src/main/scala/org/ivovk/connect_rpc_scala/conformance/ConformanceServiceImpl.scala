package org.ivovk.connect_rpc_scala.conformance

import cats.effect.Async
import cats.implicits.*
import com.google.protobuf.ByteString
import connectrpc.conformance.v1.*
import io.grpc.{Metadata, Status, StatusRuntimeException}
import org.slf4j.LoggerFactory
import scalapb.TextFormat

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*


class ConformanceServiceImpl[F[_] : Async] extends ConformanceServiceFs2GrpcTrailers[F, Metadata] {

  import org.ivovk.connect_rpc_scala.Mappings.*

  private val logger = LoggerFactory.getLogger(getClass)

  override def unary(request: UnaryRequest, ctx: Metadata): F[(UnaryResponse, Metadata)] = {
    val responseDefinition = request.getResponseDefinition

    val trailers = new Metadata()
    responseDefinition.responseTrailers.foreach { h =>
      val key = Metadata.Key.of(h.name, Metadata.ASCII_STRING_MARSHALLER)
      h.value.foreach(v => trailers.put(key, v))
    }

    val responseData = responseDefinition.response match {
      case UnaryResponseDefinition.Response.ResponseData(bs) =>
        bs.some
      case UnaryResponseDefinition.Response.Empty =>
        none
      case UnaryResponseDefinition.Response.Error(Error(code, message, _)) =>
        val status = Status.fromCodeValue(code.value)
          .withDescription(message.orNull)
          .augmentDescription(
            TextFormat.printToSingleLineUnicodeString(
              ConformancePayload.RequestInfo(
                requests = Seq(request.toProtoAny)
              ).toProtoAny
            )
          )

        throw new StatusRuntimeException(status, trailers)
    }

    val timeout = Option(ctx.get(Metadata.Key.of("grpc-timeout", Metadata.ASCII_STRING_MARSHALLER)))
      .map(v => v.substring(0, v.length - 1).toLong / 1000)

    val payload = ConformancePayload(
      data = responseData.getOrElse(ByteString.EMPTY),
      requestInfo = ConformancePayload.RequestInfo(
        requestHeaders = mkConformanceHeaders(ctx),
        timeoutMs = timeout,
        requests = Seq(request.toProtoAny),
        connectGetInfo = None,
      ).some
    )

    Async[F].sleep(Duration(responseDefinition.responseDelayMs, TimeUnit.MILLISECONDS)) *>
      Async[F].pure((UnaryResponse(payload.some), trailers))
  }

  private def mkConformanceHeaders(metadata: Metadata): Seq[Header] = {
    metadata.keys().asScala.map { key =>
      Header(key, metadata.getAll(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)).asScala.toSeq)
    }.toSeq
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

  override def idempotentUnary(
    request: IdempotentUnaryRequest,
    ctx: Metadata
  ): F[(IdempotentUnaryResponse, Metadata)] = ???
}
