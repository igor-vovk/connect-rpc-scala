package org.ivovk.connect_rpc_scala.http4s.connect

import cats.effect.Async
import cats.implicits.*
import io.grpc.{Status as GrpcStatus, *}
import io.grpc.MethodDescriptor.MethodType
import org.http4s.{Headers, Response, Status}
import org.ivovk.connect_rpc_scala.grpc.{ClientCalls, GrpcHeaders, MethodRegistry}
import org.ivovk.connect_rpc_scala.http.MetadataToHeaders
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, EntityToDecode, MessageCodec}
import org.ivovk.connect_rpc_scala.http4s.ErrorHandler
import org.ivovk.connect_rpc_scala.http4s.ResponseBuilder.*
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage

import scala.concurrent.duration.*

class ConnectHandler[F[_]: Async](
  channel: Channel,
  errorHandler: ErrorHandler[F],
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

    val f = method.descriptor.getType match
      case MethodType.UNARY =>
        handleUnary(req, method)
      case MethodType.CLIENT_STREAMING =>
        handleClientStreaming(req, method)
      case unsupported =>
        Async[F].raiseError(
          GrpcStatus.UNIMPLEMENTED.withDescription(s"Unsupported method type: $unsupported").asException()
        )

    f.handleErrorWith(errorHandler.handle)
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
      .asyncUnaryCall(
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
      .streamingCall(
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

        mkStreamingResponse(
          headers,
          fs2.Stream(
            response.value,
            connectrpc.Error(),
          ),
        )
      }
  }

}
