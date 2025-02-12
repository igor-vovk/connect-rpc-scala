package org.ivovk.connect_rpc_scala.http4s.transcoding

import cats.effect.Async
import cats.implicits.*
import io.grpc.*
import org.http4s.Response
import org.http4s.Status.Ok
import org.ivovk.connect_rpc_scala.grpc.{ClientCalls, GrpcHeaders, MethodRegistry}
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import org.ivovk.connect_rpc_scala.http4s.ResponseExtensions.*
import org.ivovk.connect_rpc_scala.http4s.{ErrorHandler, Http4sHeaderMapping}
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage

import scala.concurrent.duration.*

class TranscodingHandler[F[_]: Async](
  channel: Channel,
  errorHandler: ErrorHandler[F],
  headerMapping: Http4sHeaderMapping,
) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def handleUnary(
    message: GeneratedMessage,
    headers: Metadata,
    method: MethodRegistry.Entry,
  )(using MessageCodec[F], EncodeOptions): F[Response[F]] = {
    if (logger.isTraceEnabled) {
      // Used in conformance tests
      Option(headers.get(GrpcHeaders.XTestCaseNameKey)) match {
        case Some(header) =>
          logger.trace(s">>> Test Case: ${header.value}")
        case None => // ignore
      }
    }

    if (logger.isTraceEnabled) {
      logger.trace(s">>> Method: ${method.descriptor.getFullMethodName}")
    }

    val callOptions = CallOptions.DEFAULT
      .pipeIfDefined(Option(headers.get(GrpcHeaders.ConnectTimeoutMsKey))) { (options, timeout) =>
        options.withDeadlineAfter(timeout.value, MILLISECONDS)
      }

    ClientCalls
      .asyncUnaryCall(
        channel,
        method.descriptor,
        callOptions,
        headers,
        message,
      )
      .map { response =>
        val headers = headerMapping.toHeaders(response.headers) ++
          headerMapping.toHeaders(response.trailers)

        if (logger.isTraceEnabled) {
          logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
        }

        Response(Ok, headers = headers).withMessage(response.value)
      }
      .handleErrorWith(errorHandler.handle)
  }

}
