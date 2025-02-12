package org.ivovk.connect_rpc_scala.netty.connect

import cats.effect.Async
import cats.implicits.*
import io.grpc.*
import io.grpc.MethodDescriptor.MethodType
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.*
import org.ivovk.connect_rpc_scala.MetadataToHeaders
import org.ivovk.connect_rpc_scala.grpc.{ClientCalls, GrpcHeaders, MethodRegistry}
import org.ivovk.connect_rpc_scala.http.RequestEntity
import org.ivovk.connect_rpc_scala.http.codec.{Compressor, EncodeOptions, MessageCodec}
import org.ivovk.connect_rpc_scala.netty.ErrorHandler
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage
import scodec.bits.ByteVector

import scala.concurrent.duration.*

class ConnectHandler[F[_]: Async](
  channel: Channel,
  errorHandler: ErrorHandler[F],
  headerMapping: MetadataToHeaders[HttpHeaders],
) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

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
          new StatusException(
            io.grpc.Status.UNIMPLEMENTED.withDescription(s"Unsupported method type: $unsupported")
          )
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

        if (logger.isTraceEnabled) {
          logger.trace(s"<<< Headers: $headers")
        }

        val responseEntity = codec.encode(response.value, encodeOptions)

        responseEntity.body.compile.to(ByteVector)
          .map { byteVector =>
            val bytes = byteVector.toArray
            val httpResponse = new DefaultFullHttpResponse(
              HttpVersion.HTTP_1_1,
              HttpResponseStatus.OK,
              Unpooled.wrappedBuffer(bytes),
            )
            httpResponse.headers()
              .pipeEach(responseEntity.headers) { case (headers, (name, value)) =>
                headers.set(name, value)
              }
              .set("Content-Length", bytes.length)

            httpResponse
          }
      }
  }

}
