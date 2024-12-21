package org.ivovk.connect_rpc_scala.connect

import cats.effect.Async
import cats.implicits.*
import io.grpc.*
import io.grpc.MethodDescriptor.MethodType
import org.http4s.Status.Ok
import org.http4s.{Header, Response}
import org.ivovk.connect_rpc_scala.Mappings.*
import org.ivovk.connect_rpc_scala.grpc.{ClientCalls, MethodRegistry}
import org.ivovk.connect_rpc_scala.http.Headers.`X-Test-Case-Name`
import org.ivovk.connect_rpc_scala.http.RequestEntity
import org.ivovk.connect_rpc_scala.http.codec.{Compressor, EncodeOptions, MessageCodec}
import org.ivovk.connect_rpc_scala.{ErrorHandler, HeaderMapping}
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage

import scala.concurrent.duration.*
import scala.util.chaining.*

class ConnectHandler[F[_] : Async](
  channel: Channel,
  errorHandler: ErrorHandler[F],
  headerMapping: HeaderMapping,
) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def handle(
    req: RequestEntity[F],
    method: MethodRegistry.Entry,
  )(using MessageCodec[F]): F[Response[F]] = {
    given EncodeOptions = EncodeOptions(
      encoding = req.encoding.filter(Compressor.supportedEncodings.contains)
    )

    val f = method.descriptor.getType match
      case MethodType.UNARY =>
        handleUnary(req, method)
      case unsupported =>
        Async[F].raiseError(new StatusException(
          io.grpc.Status.UNIMPLEMENTED.withDescription(s"Unsupported method type: $unsupported")
        ))

    f.handleErrorWith(errorHandler.handle)
  }

  private def handleUnary(
    req: RequestEntity[F],
    method: MethodRegistry.Entry,
  )(using MessageCodec[F], EncodeOptions): F[Response[F]] = {
    if (logger.isTraceEnabled) {
      // Used in conformance tests
      req.headers.get[`X-Test-Case-Name`] match {
        case Some(header) =>
          logger.trace(s">>> Test Case: ${header.value}")
        case None => // ignore
      }
    }

    req.as[GeneratedMessage](using method.requestMessageCompanion)
      .flatMap { message =>
        if (logger.isTraceEnabled) {
          logger.trace(s">>> Method: ${method.descriptor.getFullMethodName}")
        }

        val callOptions = CallOptions.DEFAULT
          .pipe(
            req.timeout match {
              case Some(timeout) => _.withDeadlineAfter(timeout, MILLISECONDS)
              case None => identity
            }
          )

        ClientCalls.asyncUnaryCall(
          channel,
          method.descriptor,
          callOptions,
          headerMapping.toMetadata(req.headers),
          message
        )
      }
      .map { response =>
        val headers = headerMapping.toHeaders(response.headers) ++
          headerMapping.trailersToHeaders(response.trailers)

        if (logger.isTraceEnabled) {
          logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
        }

        Response(Ok, headers = headers).withMessage(response.value)
      }
  }

}
