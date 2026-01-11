package org.ivovk.connect_rpc_scala.netty.connect

import cats.effect.Async
import cats.implicits.*
import fs2.Stream
import io.grpc.MethodDescriptor.MethodType
import io.grpc.{CallOptions, Channel, Status}
import io.netty.handler.codec.http.{HttpHeaders, HttpResponse}
import org.ivovk.connect_rpc_scala.grpc.{ClientCalls, GrpcHeaders, MethodRegistry}
import org.ivovk.connect_rpc_scala.http.MetadataToHeaders
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, EntityToDecode, MessageCodec}
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
    req: EntityToDecode[F],
    method: MethodRegistry.Entry,
  )(using MessageCodec[F]): F[HttpResponse] = {
    given EncodeOptions = EncodeOptions(
      charset = req.charset,
      encoding = req.encoding,
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
    req: EntityToDecode[F],
    method: MethodRegistry.Entry,
  )(using codec: MessageCodec[F], encodeOptions: EncodeOptions): F[HttpResponse] = {
    if (logger.isTraceEnabled) {
      // Used in conformance tests
      Option(req.headers.get(GrpcHeaders.XTestCaseNameKey)) match {
        case Some(testCase) => logger.trace(s">>> Test Case: $testCase")
        case None           => // ignore
      }
    }

    req.as[GeneratedMessage](using method.requestMessageCompanion)
      .compile.onlyOrError
      .flatMap { message =>
        if (logger.isTraceEnabled) {
          logger.trace(s">>> Method: ${method.descriptor.getFullMethodName}")
        }

        val callOptions = CallOptions.DEFAULT
          .pipeIfDefined(Option(req.headers.get(GrpcHeaders.ConnectTimeoutMsKey))) { (options, timeoutMs) =>
            options.withDeadlineAfter(timeoutMs, MILLISECONDS)
          }

        ClientCalls.asyncUnaryCall(
          channel,
          method.descriptor,
          callOptions,
          req.headers,
          Stream.emit[F, GeneratedMessage](message),
        )
      }
      .flatMap { response =>
        val headers = headerMapping.toHeaders(response.headers)
          .add(headerMapping.trailersToHeaders(response.trailers))

        Response.create(response.value, headers = headers)
      }
  }

}
