package org.ivovk.connect_rpc_scala

import cats.effect.Async
import cats.implicits.*
import io.grpc.*
import org.http4s.dsl.Http4sDsl
import org.http4s.{Header, Headers, MessageFailure, Response}
import org.ivovk.connect_rpc_scala.Mappings.*
import org.ivovk.connect_rpc_scala.grpc.{ClientCalls, GrpcHeaders, MethodRegistry}
import org.ivovk.connect_rpc_scala.http.Headers.`X-Test-Case-Name`
import org.ivovk.connect_rpc_scala.http.RequestEntity
import org.ivovk.connect_rpc_scala.http.RequestEntity.*
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.chaining.*

object TranscodingHandler {

  extension [F[_]](response: Response[F]) {
    def withMessage(entity: GeneratedMessage)(using codec: MessageCodec[F], options: EncodeOptions): Response[F] =
      codec.encode(entity, options).applyTo(response)
  }

}

class TranscodingHandler[F[_] : Async](
  channel: Channel,
  httpDsl: Http4sDsl[F],
  treatTrailersAsHeaders: Boolean,
) {

  import TranscodingHandler.*
  import httpDsl.*

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
        headers.toMetadata,
        message
      )
      .map { response =>
        val headers = response.headers.toHeaders() ++
          response.trailers.toHeaders(trailing = !treatTrailersAsHeaders)

        if (logger.isTraceEnabled) {
          logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
        }

        Response(Ok, headers = headers).withMessage(response.value)
      }
      .handleError { e =>
        val grpcStatus = e match {
          case e: StatusException =>
            e.getStatus.getDescription match {
              case "an implementation is missing" => io.grpc.Status.UNIMPLEMENTED
              case _ => e.getStatus
            }
          case e: StatusRuntimeException => e.getStatus
          case _: MessageFailure => io.grpc.Status.INVALID_ARGUMENT
          case _ => io.grpc.Status.INTERNAL
        }

        val (message, metadata) = e match {
          case e: StatusRuntimeException => (Option(e.getStatus.getDescription), e.getTrailers)
          case e: StatusException => (Option(e.getStatus.getDescription), e.getTrailers)
          case e => (Option(e.getMessage), new Metadata())
        }

        val httpStatus  = grpcStatus.toHttpStatus
        val connectCode = grpcStatus.toConnectCode

        // Should be called before converting metadata to headers
        val details = Option(metadata.removeAll(GrpcHeaders.ErrorDetailsKey))
          .fold(Seq.empty)(_.asScala.toSeq)

        val headers = metadata.toHeaders(trailing = !treatTrailersAsHeaders)

        if (logger.isTraceEnabled) {
          logger.trace(s"<<< Http Status: $httpStatus, Connect Error Code: $connectCode")
          logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
          logger.trace(s"<<< Error processing request", e)
        }

        Response[F](httpStatus, headers = headers).withMessage(connectrpc.Error(
          code = connectCode,
          message = message,
          details = details
        ))
      }
  }

}
