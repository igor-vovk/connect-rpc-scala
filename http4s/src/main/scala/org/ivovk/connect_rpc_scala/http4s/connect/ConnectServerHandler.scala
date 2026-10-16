package org.ivovk.connect_rpc_scala.http4s.connect

import cats.effect.Async
import cats.implicits.*
import fs2.Stream
import io.grpc.MethodDescriptor.MethodType
import io.grpc.{Status as GrpcStatus, *}
import org.http4s.{Header, Headers, Response, Status}
import org.ivovk.connect_rpc_scala.connect.ErrorHandling
import org.ivovk.connect_rpc_scala.grpc.{ClientCalls, GrpcHeaders, MethodRegistry}
import org.ivovk.connect_rpc_scala.grpc.ClientCalls.StreamingResult
import org.ivovk.connect_rpc_scala.http.HeaderMapping.cachedAsciiKey
import org.ivovk.connect_rpc_scala.http.MetadataToHeaders
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, EntityToDecode, MessageCodec}
import org.ivovk.connect_rpc_scala.http4s.ResponseBuilder.*
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.{Logger, LoggerFactory}
import org.typelevel.ci.CIString
import scalapb.GeneratedMessage

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class ConnectServerHandler[F[_]: Async](
  channel: Channel,
  headerMapping: MetadataToHeaders[Headers],
) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def handle(
    req: EntityToDecode[F],
    method: MethodRegistry.Entry,
  )(using MessageCodec[F]): F[Response[F]] = {
    given EncodeOptions = EncodeOptions(
      charset = req.charset,
      encoding = req.encoding,
    )

    if (logger.isTraceEnabled) {
      // Used in conformance tests
      Option(req.headers.get(GrpcHeaders.XTestCaseNameKey)) match {
        case Some(testCase) => logger.trace(s">>> Test Case: $testCase")
        case None           => // ignore
      }
    }

    method.descriptor.getType match
      case MethodType.UNARY =>
        handleUnary(req, method).handleErrorWith(handleUnaryError)
      case MethodType.CLIENT_STREAMING =>
        handleClientStreaming(req, method).handleErrorWith(handleStreamingError)
      case MethodType.SERVER_STREAMING =>
        handleStreaming(req, method).handleErrorWith(handleStreamingError)
      case MethodType.BIDI_STREAMING =>
        handleStreaming(req, method).handleErrorWith(handleStreamingError)
      case unsupported =>
        handleUnaryError(
          GrpcStatus.UNIMPLEMENTED.withDescription(s"Unsupported method type: $unsupported").asException()
        )
  }

  private def handleUnary(
    req: EntityToDecode[F],
    method: MethodRegistry.Entry,
  )(using MessageCodec[F], EncodeOptions): F[Response[F]] = {
    if (logger.isTraceEnabled) {
      logger.trace(s">>> Method: ${method.descriptor.getFullMethodName}")
    }

    val callOptions = CallOptions.DEFAULT
      .pipeIfDefined(Option(req.headers.get(GrpcHeaders.ConnectTimeoutMsKey))) { (options, timeout) =>
        options.withDeadlineAfter(timeout, MILLISECONDS)
      }

    ClientCalls
      .clientStreamingCall(
        channel,
        method.descriptor,
        callOptions,
        req.headers,
        req.as[GeneratedMessage](using method.requestMessageCompanion),
      )
      .map { response =>
        val headers = headerMapping.toHeaders(response.headers) ++
          headerMapping.trailersToHeaders(response.trailers)

        if (logger.isTraceEnabled) {
          logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
        }

        mkUnaryResponse(Status.Ok, headers, response.value)
      }
  }

  private def handleUnaryError(e: Throwable)(using MessageCodec[F]): F[Response[F]] = {
    val details = ErrorHandling.extractDetails(e)
    val headers = headerMapping.trailersToHeaders(details.trailers)

    if (logger.isTraceEnabled) {
      logger.trace(
        s"<<< Http Status: ${details.httpStatusCode}, Connect Error Code: ${details.error.code}"
      )
      logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
      logger.trace(s"<<< Error processing request", e)
    }

    val httpStatus = Status.fromInt(details.httpStatusCode).fold(throw _, identity)

    mkUnaryResponse(httpStatus, headers, details.error).pure[F]
  }

  private def handleClientStreaming(
    req: EntityToDecode[F],
    method: MethodRegistry.Entry,
  )(using MessageCodec[F], EncodeOptions): F[Response[F]] = {
    if (logger.isTraceEnabled) {
      logger.trace(s">>> Method: ${method.descriptor.getFullMethodName}")
    }

    val callOptions = CallOptions.DEFAULT
      .pipeIfDefined(Option(req.headers.get(GrpcHeaders.ConnectTimeoutMsKey))) { (options, timeout) =>
        options.withDeadlineAfter(timeout, MILLISECONDS)
      }

    ClientCalls
      .clientStreamingCall(
        channel,
        method.descriptor,
        callOptions,
        req.headers,
        req.as[GeneratedMessage](using method.requestMessageCompanion),
      )
      .map { response =>
        val headers = headerMapping.toHeaders(response.headers)

        if (logger.isTraceEnabled) {
          logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
        }

        val (responseMetadata, headersToAdd) = streamingExtractMetadataAndHeaders(response.trailers)

        mkStreamingResponse(
          headers ++ Headers(headersToAdd),
          Stream(
            response.value,
            connectrpc.EndStreamMessage(
              metadata = responseMetadata
            ),
          ),
        )
      }
  }

  private def handleStreamingError(e: Throwable)(using MessageCodec[F]): F[Response[F]] = {
    val details = ErrorHandling.extractDetails(e)
    val headers = headerMapping.toHeaders(details.headers)

    val (responseMetadata, headersToAdd) = streamingExtractMetadataAndHeaders(details.trailers)

    if (logger.isTraceEnabled) {
      logger.trace(s"<<< Error processing request", e)
      logger.trace(
        s"<<< Http Status: ${details.httpStatusCode}, Connect Error Code: ${details.error.code}"
      )
    }

    mkStreamingResponse[F](
      headers ++ Headers(headersToAdd),
      Stream(
        connectrpc.EndStreamMessage(
          error = Some(details.error),
          metadata = responseMetadata,
        )
      ),
    ).pure[F]
  }

  private def handleStreaming(
    req: EntityToDecode[F],
    method: MethodRegistry.Entry,
  )(using MessageCodec[F], EncodeOptions): F[Response[F]] = {
    val callOptions = CallOptions.DEFAULT
      .pipeIfDefined(Option(req.headers.get(GrpcHeaders.ConnectTimeoutMsKey))) { (options, timeout) =>
        options.withDeadlineAfter(timeout, MILLISECONDS)
      }

    val responseF = method.descriptor.getType match
      case MethodType.SERVER_STREAMING =>
        ClientCalls.serverStreamingCall(
          channel,
          method.descriptor,
          callOptions,
          req.headers,
          req.as[GeneratedMessage](using method.requestMessageCompanion),
        )
      case MethodType.BIDI_STREAMING =>
        ClientCalls.bidiStreamingCall(
          channel,
          method.descriptor,
          callOptions,
          req.headers,
          req.as[GeneratedMessage](using method.requestMessageCompanion),
        )
      case other =>
        GrpcStatus.UNIMPLEMENTED
          .withDescription(s"Unsupported method type for streaming response handler: $other")
          .asException()
          .raiseError[F, ClientCalls.StreamingResponse[F, GeneratedMessage]]

    responseF.map { response =>
      val headers = headerMapping.toHeaders(response.headers)

      if (logger.isTraceEnabled) {
        logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
      }

      val messages = response.messages
        .map {
          case StreamingResult.Message(message: GeneratedMessage) =>
            System.err.println(s"ConnectServerHandler.handleStreaming: forwarding message $message")
            message
          case StreamingResult.Complete(trailers) =>
            System.err.println(s"ConnectServerHandler.handleStreaming: complete trailers=$trailers")
            connectrpc.EndStreamMessage(
              metadata = streamingTrailersToMetadata(trailers)
            )
          case StreamingResult.Error(error) =>
            val details = ErrorHandling.extractDetails(error)
            System.err.println(
              s"ConnectServerHandler.handleStreaming: error code=${details.error.code} trailers=${details.trailers}"
            )

            connectrpc.EndStreamMessage(
              error = Some(details.error),
              metadata = streamingTrailersToMetadata(details.trailers),
            )
        }
        .pipeIf(logger.isTraceEnabled) {
          _.debug(el => s"<<< Frame: $el", logger.trace)
        }

      mkStreamingResponse(headers, messages)
    }
  }

  private def streamingExtractMetadataAndHeaders(
    trailers: Metadata
  ): (Seq[connectrpc.MetadataEntry], Seq[Header.Raw]) = {
    val responseMetadata = Seq.newBuilder[connectrpc.MetadataEntry]
    val headersToAdd     = Seq.newBuilder[Header.Raw]

    trailers.keys.forEach { key =>
      if key.startsWith("trailer-") then
        responseMetadata += connectrpc.MetadataEntry(
          key = key.substring("trailer-".length),
          value = trailers.getAll(cachedAsciiKey(key)).asScala.toSeq,
        )
      else
        headersToAdd += Header.Raw(
          CIString(key),
          trailers.getAll(cachedAsciiKey(key)).asScala.mkString(","),
        )
    }

    (responseMetadata.result(), headersToAdd.result())
  }

  private def streamingTrailersToMetadata(
    trailers: Metadata
  ): Seq[connectrpc.MetadataEntry] =
    trailers.keys.asScala.toSeq.map { key =>
      connectrpc.MetadataEntry(
        key = key.stripPrefix("trailer-"),
        value = trailers.getAll(cachedAsciiKey(key)).asScala.toSeq,
      )
    }

}
