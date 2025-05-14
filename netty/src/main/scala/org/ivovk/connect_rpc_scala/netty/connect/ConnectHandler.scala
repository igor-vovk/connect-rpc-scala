package org.ivovk.connect_rpc_scala.netty.connect

import cats.effect.Async
import cats.implicits.*
import io.grpc.MethodDescriptor.MethodType
import io.grpc.{CallOptions, Channel, Status}
import io.netty.handler.codec.http.{HttpHeaders, HttpResponse}
import org.ivovk.connect_rpc_scala.grpc.{ClientCalls, GrpcHeaders, MethodRegistry}
import org.ivovk.connect_rpc_scala.http.codec.{Compressor, EncodeOptions, MessageCodec}
import org.ivovk.connect_rpc_scala.http.{MetadataToHeaders, RequestEntity}
import org.ivovk.connect_rpc_scala.netty.{ErrorHandler, Response}
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.LoggerFactory
import scalapb.GeneratedMessage

import scala.concurrent.duration.*

class ConnectHandler[F[_]: Async](
  channel: Channel,
  errorHandler: ErrorHandler[F],
  headerMapping: MetadataToHeaders[HttpHeaders],
) {

  private val logger = LoggerFactory.getLogger(getClass)

  def handle(
    req: RequestEntity[F],
    method: MethodRegistry.Entry,
  )(using MessageCodec[F]): F[HttpResponse] = {
    given EncodeOptions = EncodeOptions(
      encoding = req.encoding.filter(Compressor.supportedEncodings.contains)
    )

    val f = method.descriptor.getType match
      case MethodType.UNARY =>
        handleUnary(req, method)
      case unsupported =>
        Async[F].raiseError(
          Status.UNIMPLEMENTED.withDescription(s"Unsupported method type: $unsupported").asException()
        )

    f.handleErrorWith(errorHandler.handle)
  }

  private def handleUnary(
    req: RequestEntity[F],
    method: MethodRegistry.Entry,
  )(using codec: MessageCodec[F], encodeOptions: EncodeOptions): F[HttpResponse] = {
    if (logger.isTraceEnabled) {
      // Used in conformance tests
      Option(req.headers.get(GrpcHeaders.XTestCaseNameKey)) match {
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
          .pipeIfDefined(Option(req.headers.get(GrpcHeaders.ConnectTimeoutMsKey))) { (options, header) =>
            options.withDeadlineAfter(header.value, MILLISECONDS)
          }

        ClientCalls.asyncUnaryCall(
          channel,
          method.descriptor,
          callOptions,
          req.headers,
          message,
        )
      }
      .flatMap { response =>
        val headers = headerMapping.toHeaders(response.headers)
          .add(headerMapping.trailersToHeaders(response.trailers))

        Response.create(response.value, headers = headers)
      }
  }

}
