package org.ivovk.connect_rpc_scala

import cats.data.EitherT
import cats.effect.Async
import cats.implicits.*
import io.grpc.*
import io.grpc.MethodDescriptor.MethodType
import org.http4s.dsl.Http4sDsl
import org.http4s.{Header, MediaType, MessageFailure, Method, Response}
import org.ivovk.connect_rpc_scala.Mappings.*
import org.ivovk.connect_rpc_scala.grpc.{GrpcClientCalls, GrpcHeaders, MethodName, MethodRegistry}
import org.ivovk.connect_rpc_scala.http.Headers.`X-Test-Case-Name`
import org.ivovk.connect_rpc_scala.http.codec.MessageCodec.given
import org.ivovk.connect_rpc_scala.http.codec.{MessageCodec, MessageCodecRegistry}
import org.ivovk.connect_rpc_scala.http.{MediaTypes, RequestEntity}
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.chaining.*

class ConnectHandler[F[_] : Async](
  codecRegistry: MessageCodecRegistry[F],
  methodRegistry: MethodRegistry,
  channel: Channel,
  httpDsl: Http4sDsl[F],
  treatTrailersAsHeaders: Boolean,
) {

  import httpDsl.*

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def handle(
    httpMethod: Method,
    contentType: Option[MediaType],
    entity: RequestEntity[F],
    grpcMethodName: MethodName,
  ): F[Response[F]] = {
    val eitherT = for
      given MessageCodec[F] <- EitherT.fromOptionM(
        contentType.flatMap(codecRegistry.byContentType).pure[F],
        UnsupportedMediaType(s"Unsupported content-type ${contentType.show}. " +
          s"Supported content types: ${MediaTypes.allSupported.map(_.show).mkString(", ")}")
      )

      method <- EitherT.fromOptionM(
        methodRegistry.get(grpcMethodName).pure[F],
        NotFound(connectrpc.Error(
          code = io.grpc.Status.NOT_FOUND.toConnectCode,
          message = s"Method not found: ${grpcMethodName.fullyQualifiedName}".some
        ))
      )

      _ <- EitherT.cond[F](
        // Support GET-requests for all methods until https://github.com/scalapb/ScalaPB/pull/1774 is merged
        httpMethod == Method.POST || (httpMethod == Method.GET && method.descriptor.isSafe) || true,
        (),
        Forbidden(connectrpc.Error(
          code = io.grpc.Status.PERMISSION_DENIED.toConnectCode,
          message = s"Only POST-requests are allowed for method: ${grpcMethodName.fullyQualifiedName}".some
        ))
      ).leftSemiflatMap(identity)

      response <- method.descriptor.getType match
        case MethodType.UNARY =>
          EitherT.right(handleUnary(method, entity, channel))
        case unsupported =>
          EitherT.left(NotImplemented(connectrpc.Error(
            code = io.grpc.Status.UNIMPLEMENTED.toConnectCode,
            message = s"Unsupported method type: $unsupported".some
          )))
    yield response

    eitherT.merge
  }

  private def handleUnary(
    method: MethodRegistry.Entry,
    req: RequestEntity[F],
    channel: Channel
  )(using MessageCodec[F]): F[Response[F]] = {
    if (logger.isTraceEnabled) {
      // Used in conformance tests
      req.headers.get[`X-Test-Case-Name`] match {
        case Some(header) =>
          logger.trace(s">>> Test Case: ${header.value}")
        case None => // ignore
      }
    }

    req.as[GeneratedMessage](method.requestMessageCompanion)
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

        GrpcClientCalls.asyncUnaryCall(
          channel,
          method.descriptor,
          callOptions,
          req.headers.toMetadata,
          message
        )
      }
      .map { response =>
        val headers = response.headers.toHeaders() ++
          response.trailers.toHeaders(trailing = !treatTrailersAsHeaders)

        if (logger.isTraceEnabled) {
          logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
        }

        Response(Ok, headers = headers).withEntity(response.value)
      }
      .recover { case e =>
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

        Response[F](httpStatus, headers = headers).withEntity(connectrpc.Error(
          code = connectCode,
          message = message,
          details = details
        ))
      }
  }

}
