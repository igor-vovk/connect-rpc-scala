package org.ivovk.connect_rpc_scala.conformance

import cats.implicits.*
import com.google.protobuf.ByteString
import connectrpc.conformance.v1.*
import io.grpc.{Metadata, Status, StatusException}
import scalapb.TextFormat
import scalapb.zio_grpc.{RequestContext, SafeMetadata}
import zio.{Duration, IO, UIO, ZIO, stream}

import scala.jdk.CollectionConverters.*

case class UnaryHandlerResponse(payload: ConformancePayload, trailers: Metadata)

object ConformanceServiceImpl extends ZioService.RCConformanceService {

  import org.ivovk.connect_rpc_scala.Mappings.*

  override def unary(
    request: UnaryRequest,
    ctx: RequestContext
  ): IO[StatusException, UnaryResponse] =
    for
      res <- handleUnaryRequest(
        request.getResponseDefinition,
        Seq(request.toProtoAny),
        ctx
      )
    yield UnaryResponse(res.payload.some)

  override def idempotentUnary(
    request: IdempotentUnaryRequest,
    ctx: RequestContext,
  ): IO[StatusException, IdempotentUnaryResponse] =
    for
      res <- handleUnaryRequest(
        request.getResponseDefinition,
        Seq(request.toProtoAny),
        ctx
      )
    yield IdempotentUnaryResponse(res.payload.some)

  private def handleUnaryRequest(
    responseDefinition: UnaryResponseDefinition,
    requests: Seq[com.google.protobuf.any.Any],
    ctx: RequestContext,
  ): IO[StatusException, UnaryHandlerResponse] =
    for
      requestHeaders <- mkConformanceHeaders(ctx.metadata)
      maybeTimeout <- extractTimeout(ctx.metadata)
      requestInfo = ConformancePayload.RequestInfo(
        requestHeaders = requestHeaders,
        timeoutMs = maybeTimeout,
        requests = requests
      )
      responseData <- responseDefinition.response match {
        case UnaryResponseDefinition.Response.ResponseData(bs) =>
          ZIO.some(bs)
        case UnaryResponseDefinition.Response.Empty =>
          ZIO.none
        case UnaryResponseDefinition.Response.Error(Error(code, message, _)) =>
          val status = Status.fromCodeValue(code.value)
            .withDescription(message.orNull)
            .augmentDescription(
              TextFormat.printToSingleLineUnicodeString(requestInfo.toProtoAny)
            )

          ZIO.fail(new StatusException(status))
      }

      headers = mkMetadata(responseDefinition.responseHeaders)
      trailers = mkMetadata(responseDefinition.responseTrailers)

      _ <- ZIO.sleep(Duration.fromMillis(responseDefinition.responseDelayMs))

      _ <- ctx.responseMetadata.wrap(_.merge(trailers))
    yield
      UnaryHandlerResponse(
        payload = ConformancePayload(
          responseData.getOrElse(ByteString.EMPTY),
          requestInfo.some
        ),
        trailers = trailers
      )

  // This endpoint must stay unimplemented
  override def unimplemented(
    request: UnimplementedRequest,
    ctx: RequestContext
  ): IO[StatusException, UnimplementedResponse] = ???

  private def keyof(key: String): Metadata.Key[String] =
    Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)

  private def mkConformanceHeaders(metadata: SafeMetadata): UIO[Seq[Header]] =
    metadata.wrap { metadata =>
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

  private def extractTimeout(metadata: SafeMetadata): UIO[Option[Long]] = {
    metadata.get(keyof("grpc-timeout"))
      .map(_.map(v => v.substring(0, v.length - 1).toLong / 1000))
  }


  override def serverStream(
    request: ServerStreamRequest,
    context: RequestContext
  ): stream.Stream[StatusException, ServerStreamResponse] = ???

  override def clientStream(
    request: stream.Stream[StatusException, ClientStreamRequest],
    context: RequestContext
  ): IO[StatusException, ClientStreamResponse] = ???

  override def bidiStream(
    request: stream.Stream[StatusException, BidiStreamRequest],
    context: RequestContext
  ): stream.Stream[StatusException, BidiStreamResponse] = ???
}
