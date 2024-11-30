package org.ivovk.connect_rpc_scala

import cats.Endo
import cats.data.EitherT
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.implicits.*
import fs2.compression.Compression
import io.grpc.*
import io.grpc.MethodDescriptor.MethodType
import io.grpc.stub.MetadataUtils
import org.http4s.*
import org.http4s.dsl.Http4sDsl
import org.ivovk.connect_rpc_scala.http.*
import org.ivovk.connect_rpc_scala.http.Headers.`X-Test-Case-Name`
import org.ivovk.connect_rpc_scala.http.MessageCodec.given
import org.ivovk.connect_rpc_scala.http.QueryParams.*
import org.slf4j.{Logger, LoggerFactory}
import scalapb.grpc.ClientCalls
import scalapb.json4s.{JsonFormat, Printer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, TextFormat}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.util.chaining.*

case class Configuration(
  jsonPrinterConfiguration: Endo[Printer] = identity,
  waitForShutdown: Duration = 10.seconds,
)

object ConnectRpcHttpRoutes {

  import Mappings.*

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def create[F[_] : Async](
    services: Seq[ServerServiceDefinition],
    configuration: Configuration = Configuration()
  ): Resource[F, HttpRoutes[F]] = {
    val dsl = Http4sDsl[F]
    import dsl.*

    val jsonPrinter = configuration.jsonPrinterConfiguration(JsonFormat.printer)

    val codecRegistry = MessageCodecRegistry[F](
      JsonMessageCodec[F](jsonPrinter),
      ProtoMessageCodec[F],
    )

    val methodRegistry = MethodRegistry(services)

    for
      ipChannel <- InProcessChannelBridge.create(services, configuration.waitForShutdown)
    yield
      def handle(
        httpMethod: Method,
        contentType: Option[MediaType],
        entity: RequestEntity[F],
        grpcMethod: String,
      ): F[Response[F]] = {
        val eitherT = for
          given MessageCodec[F] <- EitherT.fromOptionM(
            contentType.flatMap(codecRegistry.byContentType).pure[F],
            UnsupportedMediaType(s"Unsupported content-type ${contentType.show}. " +
              s"Supported content types: ${MediaTypes.allSupported.map(_.show).mkString(", ")}")
          )

          method <- EitherT.fromOptionM(
            methodRegistry.get(grpcMethod).pure[F],
            NotFound(connectrpc.Error(
              code = io.grpc.Status.NOT_FOUND.toConnectCode,
              message = s"Method not found: $grpcMethod".some
            ))
          )

          _ <- EitherT.cond[F](
            // Support GET-requests for all methods until https://github.com/scalapb/ScalaPB/pull/1774 is merged
            httpMethod == Method.POST || (httpMethod == Method.GET && method.descriptor.isSafe) || true,
            (),
            Forbidden(connectrpc.Error(
              code = io.grpc.Status.PERMISSION_DENIED.toConnectCode,
              message = s"Only POST-requests are allowed for method: $grpcMethod".some
            ))
          ).leftSemiflatMap(identity)

          response <- method.descriptor.getType match
            case MethodType.UNARY =>
              EitherT.right(handleUnary(dsl, method, entity, ipChannel))
            case unsupported =>
              EitherT.left(NotImplemented(connectrpc.Error(
                code = io.grpc.Status.UNIMPLEMENTED.toConnectCode,
                message = s"Unsupported method type: $unsupported".some
              )))
        yield response

        eitherT.merge
      }

      HttpRoutes.of[F] {
        case req@Method.GET -> Root / serviceName / methodName :? EncodingQP(contentType) +& MessageQP(message) =>
          val grpcMethod = grpcMethodName(serviceName, methodName)
          val entity     = RequestEntity[F](message, req.headers)

          handle(Method.GET, contentType.some, entity, grpcMethod)
        case req@Method.POST -> Root / serviceName / methodName =>
          val grpcMethod  = grpcMethodName(serviceName, methodName)
          val contentType = req.contentType.map(_.mediaType)
          val entity      = RequestEntity[F](req)

          handle(Method.POST, contentType, entity, grpcMethod)
      }
  }

  private def handleUnary[F[_] : Async](
    dsl: Http4sDsl[F],
    method: MethodRegistry.Entry,
    req: RequestEntity[F],
    channel: Channel
  )(using codec: MessageCodec[F]): F[Response[F]] = {
    import dsl.*

    if (logger.isTraceEnabled) {
      // Used in conformance tests
      req.headers.get[`X-Test-Case-Name`] match {
        case Some(header) =>
          logger.trace(s">>> Test Case: ${header.value}")
        case None => // ignore
      }
    }

    given GeneratedMessageCompanion[GeneratedMessage] = method.requestMessageCompanion

    req.as[GeneratedMessage]
      .flatMap { message =>
        val responseHeaderMetadata  = new AtomicReference[Metadata]()
        val responseTrailerMetadata = new AtomicReference[Metadata]()

        if (logger.isTraceEnabled) {
          logger.trace(s">>> Method: ${method.descriptor.getFullMethodName}, Entity: $message")
        }

        Async[F].fromFuture(Async[F].delay {
          ClientCalls.asyncUnaryCall[GeneratedMessage, GeneratedMessage](
            ClientInterceptors.intercept(
              channel,
              MetadataUtils.newAttachHeadersInterceptor(req.headers.toMetadata),
              MetadataUtils.newCaptureMetadataInterceptor(responseHeaderMetadata, responseTrailerMetadata),
            ),
            method.descriptor,
            CallOptions.DEFAULT
              .pipe(
                req.timeout.fold[Endo[CallOptions]](identity) { timeout =>
                  _.withDeadlineAfter(timeout, MILLISECONDS)
                }
              ),
            message
          )
        }).map { response =>
          val headers = org.http4s.Headers.empty ++
            responseHeaderMetadata.get.toHeaders ++
            responseTrailerMetadata.get.toTrailingHeaders

          if (logger.isTraceEnabled) {
            logger.trace(s"<<< Headers: ${headers.redactSensitive}")
          }

          Response(Ok, headers = headers).withEntity(response)
        }
      }
      .recover { case e =>
        val grpcStatus = e match {
          case e: StatusRuntimeException =>
            e.getStatus.getDescription match {
              case "an implementation is missing" => io.grpc.Status.UNIMPLEMENTED
              case _ => e.getStatus
            }
          case e: StatusException => e.getStatus
          case _ => io.grpc.Status.INTERNAL
        }

        val rawMessage = Option(e match {
          case e: StatusRuntimeException => e.getStatus.getDescription
          case e: StatusException => e.getStatus.getDescription
          case e => e.getMessage
        })

        val messageWithDetails = rawMessage
          .map(
            _.split("\n").partition(m => !m.startsWith("type_url: "))
          )
          .map((messageParts, additionalDetails) =>
            val details = additionalDetails
              .map(TextFormat.fromAscii(com.google.protobuf.any.Any, _) match {
                case Right(details) => details
                case Left(e) =>
                  logger.warn(s"Failed to parse additional details", e)

                  com.google.protobuf.wrappers.StringValue(e.msg).toProtoAny
              })
              .toSeq

            (messageParts.mkString("\n"), details)
          )

        val message = messageWithDetails.map(_._1)
        val details = messageWithDetails.map(_._2).getOrElse(Seq.empty)

        val httpStatus  = grpcStatus.toHttpStatus
        val connectCode = grpcStatus.toConnectCode

        if (logger.isTraceEnabled) {
          logger.warn(s"<<< Error processing request", e)
          logger.trace(s"<<< Http Status: $httpStatus, Connect Error Code: $connectCode, Message: ${rawMessage.orNull}")
        }

        Response[F](httpStatus).withEntity(connectrpc.Error(
          code = connectCode,
          message = messageWithDetails.map(_._1),
          details = Seq.empty // details
        ))
      }
  }

  private inline def grpcMethodName(service: String, method: String): String = service + "/" + method

}
