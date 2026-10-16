package org.ivovk.connect_rpc_scala.conformance

import cats.effect.Async
import cats.implicits.*
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import connectrpc.conformance.v1.*
import connectrpc.conformance.v1.UnaryResponseDefinition.Response
import fs2.{Pull, Stream}
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

    val bodyStream =
      Stream.emits(responseDefinition.responseData)
        .zip(Stream(Some(requestInfo)) ++ Stream.constant(None)) // Only first frame gets requestInfo
        .flatMap { (data, requestInfo) =>
          Stream.sleep[F](responseDefinition.responseDelayMs.millis) >>
            Stream.emit(ServerStreamResponse(ConformancePayload(data, requestInfo).some))
        }

    val maybeError = responseDefinition.error match
      case Some(err) if responseDefinition.responseData.isEmpty =>
        Stream.raiseError(streamingStatusRuntimeExceptionFromError(err).withDetails(requestInfo))
      case Some(err) =>
        Stream.raiseError(streamingStatusRuntimeExceptionFromError(err))
      case None      => Stream.empty

    bodyStream ++ maybeError
  }

  override def bidiStream(
    request: Stream[F, BidiStreamRequest],
    ctx: Metadata,
  ): Stream[F, BidiStreamResponse] =
    request.pull.uncons1.flatMap {
      case None               =>
        Pull.eval(F.delay(System.err.println("bidiStream: no first request"))) >>
          Pull.done
      case Some((first, rest)) =>
        val responses = {
          val responseDefinition = first.getResponseDefinition

          if first.fullDuplex then
            fullDuplexBidiResponses(first, rest, ctx, responseDefinition)
          else
            Stream.eval((Stream.emit(first) ++ rest).compile.toVector)
              .flatMap(reqs => halfDuplexBidiResponses(reqs, ctx, responseDefinition))
        }

        Pull.eval(
          F.delay(
            System.err.println(
              s"bidiStream: first request fullDuplex=${first.fullDuplex} responses=${first.getResponseDefinition.responseData.size}"
            )
          )
        ) >> responses.pull.echo
    }.stream

  // This endpoint must stay unimplemented
  override def unimplemented(
    request: UnimplementedRequest,
    ctx: Metadata,
  ): F[(UnimplementedResponse, Metadata)] = ???

  private def statusRuntimeExceptionFromError(
    error: Error,
    trailers: Metadata,
  ): StatusRuntimeException = {
    val status = Status.fromCodeValue(error.code.value)
      .withDescription(error.message.orNull)

    StatusRuntimeException(status, trailers)
  }

  private def streamingStatusRuntimeExceptionFromError(
    error: Error
  ): StatusRuntimeException =
    statusRuntimeExceptionFromError(error, new Metadata())

  private def halfDuplexBidiResponses(
    requests: Seq[BidiStreamRequest],
    ctx: Metadata,
    responseDefinition: StreamResponseDefinition,
  ): Stream[F, BidiStreamResponse] = {
    val requestInfo = mkRequestInfo(ctx, requests.map(Any.pack))

    val bodyStream =
      Stream.emits(responseDefinition.responseData.zipWithIndex)
        .flatMap { (data, index) =>
          Stream.sleep[F](responseDefinition.responseDelayMs.millis) >>
            Stream.emit(
              BidiStreamResponse(
                ConformancePayload(
                  data,
                  if index == 0 then requestInfo.some else None,
                ).some
              )
            )
        }

    val maybeError = responseDefinition.error match
      case Some(err) if responseDefinition.responseData.isEmpty =>
        Stream.raiseError(streamingStatusRuntimeExceptionFromError(err).withDetails(requestInfo))
      case Some(err) =>
        Stream.raiseError(streamingStatusRuntimeExceptionFromError(err))
      case None      => Stream.empty

    bodyStream ++ maybeError
  }

  private def fullDuplexBidiResponses(
    first: BidiStreamRequest,
    rest: Stream[F, BidiStreamRequest],
    ctx: Metadata,
    responseDefinition: StreamResponseDefinition,
  ): Stream[F, BidiStreamResponse] = {
    val responses = responseDefinition.responseData.toList

    def loop(
      remainingRequests: Stream[F, BidiStreamRequest],
      remainingResponses: List[ByteString],
      includeTransportInfo: Boolean,
    ): Pull[F, BidiStreamResponse, Unit] =
      remainingRequests.pull.uncons1.flatMap {
        case None =>
          responseDefinition.error match
            case Some(err) if remainingResponses.isEmpty =>
              Pull.raiseError(streamingStatusRuntimeExceptionFromError(err))
            case _ =>
              Pull.done

        case Some((req, tail)) =>
          remainingResponses match
            case data :: nextResponses =>
              val requestInfo = mkRequestInfo(
                ctx,
                Seq(Any.pack(req)),
                includeTransportInfo = includeTransportInfo,
              )

              Pull.eval(F.sleep(responseDefinition.responseDelayMs.millis)) >>
                Pull.output1(
                  BidiStreamResponse(
                    ConformancePayload(data, requestInfo.some).some
                  )
                ) >>
                loop(tail, nextResponses, includeTransportInfo = false)

            case Nil =>
              responseDefinition.error match
                case Some(err) =>
                  val requestInfo = mkRequestInfo(
                    ctx,
                    Seq(Any.pack(req)),
                    includeTransportInfo = includeTransportInfo,
                  )

                  val error =
                    if remainingResponses == responses then
                      streamingStatusRuntimeExceptionFromError(err).withDetails(requestInfo)
                    else
                      streamingStatusRuntimeExceptionFromError(err)

                  Pull.raiseError(error)
                case None =>
                  Pull.done
      }

    loop(Stream.emit(first) ++ rest, responses, includeTransportInfo = true).stream
  }

  private def mkRequestInfo(
    ctx: Metadata,
    requests: Seq[Any],
    includeTransportInfo: Boolean = true,
  ): ConformancePayload.RequestInfo =
    ConformancePayload.RequestInfo(
      requestHeaders = if includeTransportInfo then toHeaderSeq(ctx) else Seq.empty,
      timeoutMs = if includeTransportInfo then extractTimeoutMs(ctx) else None,
      requests = requests,
    )

}
