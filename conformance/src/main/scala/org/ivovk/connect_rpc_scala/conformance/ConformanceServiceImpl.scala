package org.ivovk.connect_rpc_scala.conformance

import cats.effect.Async
import cats.implicits.*
import connectrpc.conformance.v1.*
import io.grpc.{Metadata, Status, StatusRuntimeException}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*


class ConformanceServiceImpl[F[_] : Async] extends ConformanceServiceFs2GrpcTrailers[F, Metadata] {

  override def unary(request: UnaryRequest, ctx: Metadata): F[(UnaryResponse, Metadata)] = {
    val responseDefinition = request.getResponseDefinition

    val trailers = new Metadata()
    responseDefinition.responseTrailers.foreach { h =>
      val key = Metadata.Key.of(h.name, Metadata.ASCII_STRING_MARSHALLER)
      h.value.foreach(v => trailers.put(key, v))
    }

    responseDefinition.response match {
      case UnaryResponseDefinition.Response.ResponseData(bs) =>
        val response = UnaryResponse(
          payload = ConformancePayload(
            data = bs,
            requestInfo = ConformancePayload.RequestInfo(
              requestHeaders = ctx.keys().asScala.map { key =>
                Header(key, ctx.getAll(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)).asScala.toSeq)
              }.toSeq,
              timeoutMs = None,
              requests = Seq(
//                com.google.protobuf.any.Any(
//                  typeUrl = "type.googleapis.com/connectrpc.conformance.v1.UnaryRequest",
//                  value = request.toByteString
//                )
              ),
              connectGetInfo = None,
            ).some
          ).some
        )

        Async[F].sleep(Duration(responseDefinition.responseDelayMs, TimeUnit.MILLISECONDS)) *>
          Async[F].pure((response, trailers))
      case UnaryResponseDefinition.Response.Error(Error(code, message, details)) =>
        val status     = Status.fromCodeValue(code.value).withDescription(message.orNull)
        val augmStatus = details.foldLeft(status) { (s, d) =>
          s.augmentDescription(d.toProtoString)
        }

        throw new StatusRuntimeException(augmStatus, trailers)
      case _ =>
        throw new RuntimeException("Unknown response type")
    }
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

  override def unimplemented(
    request: UnimplementedRequest,
    ctx: Metadata
  ): F[(UnimplementedResponse, Metadata)] = ???

  override def idempotentUnary(
    request: IdempotentUnaryRequest,
    ctx: Metadata
  ): F[(IdempotentUnaryResponse, Metadata)] = ???
}
