package org.ivovk.connect_rpc_scala.transcoding

import cats.effect.Async
import cats.implicits.*
import io.grpc.*
import org.http4s.Status.Ok
import org.http4s.{Header, Headers, Response}
import org.ivovk.connect_rpc_scala.ErrorHandler
import org.ivovk.connect_rpc_scala.Mappings.*
import org.ivovk.connect_rpc_scala.grpc.{ClientCalls, MethodRegistry}
import org.ivovk.connect_rpc_scala.http.Headers.`X-Test-Case-Name`
import org.ivovk.connect_rpc_scala.http.RequestEntity
import org.ivovk.connect_rpc_scala.http.RequestEntity.*
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage

import scala.concurrent.duration.*
import scala.util.chaining.*

class TranscodingHandler[F[_] : Async](
  channel: Channel,
  errorHandler: ErrorHandler[F],
  incomingHeadersFilter: String => Boolean,
) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def handleUnary(
    message: GeneratedMessage,
    headers: Headers,
    method: MethodRegistry.Entry,
  )(using MessageCodec[F], EncodeOptions): F[Response[F]] = {
    if (logger.isTraceEnabled) {
      // Used in conformance tests
      headers.get[`X-Test-Case-Name`] match {
        case Some(header) =>
          logger.trace(s">>> Test Case: ${header.value}")
        case None => // ignore
      }
    }

    if (logger.isTraceEnabled) {
      logger.trace(s">>> Method: ${method.descriptor.getFullMethodName}")
    }

    val callOptions = CallOptions.DEFAULT
      .pipe(
        headers.timeout match {
          case Some(timeout) => _.withDeadlineAfter(timeout, MILLISECONDS)
          case None => identity
        }
      )

    ClientCalls
      .asyncUnaryCall(
        channel,
        method.descriptor,
        callOptions,
        headers.toMetadata(incomingHeadersFilter),
        message
      )
      .map { response =>
        val headers = response.headers.toHeaders() ++ response.trailers.toHeaders()

        if (logger.isTraceEnabled) {
          logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
        }

        Response(Ok, headers = headers).withMessage(response.value)
      }
      .handleErrorWith(errorHandler.handle)
  }

}
