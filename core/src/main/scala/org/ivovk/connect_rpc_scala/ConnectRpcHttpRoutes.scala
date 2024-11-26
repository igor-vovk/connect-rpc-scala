package org.ivovk.connect_rpc_scala

import cats.Endo
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.implicits.*
import fs2.compression.Compression
import io.grpc.*
import io.grpc.MethodDescriptor.MethodType
import io.grpc.stub.MetadataUtils
import org.http4s.*
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.ivovk.connect_rpc_scala.http.*
import org.ivovk.connect_rpc_scala.http.MessageEncoder.given_EntityEncoder_F_A
import org.slf4j.{Logger, LoggerFactory}
import org.typelevel.ci.CIStringSyntax
import scalapb.grpc.ClientCalls
import scalapb.json4s.{JsonFormat, Printer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, TextFormat}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

case class Configuration(
  jsonPrinterConfiguration: Endo[Printer] = identity,
  waitForShutdown: Duration = 10.seconds,
)

object ConnectRpcHttpRoutes {

  import Mappings.*

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private case class RegistryEntry(
    requestMessageCompanion: GeneratedMessageCompanion[GeneratedMessage],
    methodDescriptor: MethodDescriptor[GeneratedMessage, GeneratedMessage],
  )

  def create[F[_] : Async](
    services: Seq[ServerServiceDefinition],
    configuration: Configuration = Configuration()
  ): Resource[F, HttpRoutes[F]] = {
    val dsl = Http4sDsl[F]
    import dsl.*

    val jsonPrinter = configuration.jsonPrinterConfiguration(JsonFormat.printer)

    val coderRegistry = MessageCoderRegistry[F](
      MCEntry(JsonMessageEncoder[F](jsonPrinter), JsonMessageDecoder[F]),
      MCEntry(ProtoMessageEncoder[F], ProtoMessageDecoder[F]),
    )

    val methodRegistry = services
      .flatMap(_.getMethods.asScala)
      .map(_.asInstanceOf[ServerMethodDefinition[GeneratedMessage, GeneratedMessage]])
      .map { smd =>
        val methodDescriptor = smd.getMethodDescriptor

        val requestMarshaller = methodDescriptor.getRequestMarshaller match
          case m: scalapb.grpc.Marshaller[_] => m
          case tm: scalapb.grpc.TypeMappedMarshaller[_, _] => tm
          case unsupported => throw new RuntimeException(s"Unsupported marshaller $unsupported")

        val companionField = requestMarshaller.getClass.getDeclaredField("companion")
        companionField.setAccessible(true)

        val requestCompanion = companionField.get(requestMarshaller)
          .asInstanceOf[GeneratedMessageCompanion[GeneratedMessage]]

        val entry = RegistryEntry(
          requestMessageCompanion = requestCompanion,
          methodDescriptor = methodDescriptor,
        )

        methodDescriptor.getFullMethodName -> entry
      }
      .toMap

    for
      ipChannel <- InProcessChannelBridge.create(services, configuration.waitForShutdown)
    yield
      val httpApp = HttpRoutes.of[F] {
        case req@Method.POST -> Root / serviceName / methodName =>
          val grpcMethod  = grpcMethodName(serviceName, methodName)
          val contentType = req.headers.get[`Content-Type`].map(_.mediaType)

          contentType.flatMap(coderRegistry.fromContentType) match {
            case Some(entry) =>
              given MessageEncoder[F] = entry.encoder

              given MessageDecoder[F] = entry.decoder

              methodRegistry.get(grpcMethod) match {
                case Some(entry) =>
                  entry.methodDescriptor.getType match
                    case MethodType.UNARY =>
                      handleUnary(dsl, entry, req, ipChannel)
                    case unsupported =>
                      NotImplemented(connectrpc.Error(
                        code = io.grpc.Status.UNIMPLEMENTED.toConnectCode,
                        message = s"Unsupported method type: $unsupported".some
                      ))
                case None =>
                  NotFound(connectrpc.Error(
                    code = io.grpc.Status.NOT_FOUND.toConnectCode,
                    message = s"Method not found: $grpcMethod".some
                  ))
              }
            case None =>
              UnsupportedMediaType(s"Unsupported Content-Type header ${contentType.map(_.show).orNull}")
          }
      }

      httpApp
  }


  private def handleUnary[F[_] : Async](
    dsl: Http4sDsl[F],
    entry: ConnectRpcHttpRoutes.RegistryEntry,
    req: Request[F],
    channel: Channel
  )(using encoder: MessageEncoder[F], decoder: MessageDecoder[F]): F[Response[F]] = {
    import dsl.*

    req.headers.get(ci"X-Test-Case-Name") match {
      case Some(headers) =>
        logger.trace(s">>> Test Case: ${headers.head.value}")
      case None => // ignore
    }

    given GeneratedMessageCompanion[GeneratedMessage] = entry.requestMessageCompanion

    given EntityDecoder[F, GeneratedMessage] = EntityDecoder.decodeBy(MediaRange.`application/*`)(decoder.decode)

    req.as[GeneratedMessage]
      .flatMap { message =>
        val responseHeaderMetadata  = new AtomicReference[Metadata]()
        val responseTrailerMetadata = new AtomicReference[Metadata]()

        logger.trace(s">>> Method: ${entry.methodDescriptor.getFullMethodName}, Entity: $message")

        Async[F].fromFuture(Async[F].delay {
          ClientCalls.asyncUnaryCall[GeneratedMessage, GeneratedMessage](
            ClientInterceptors.intercept(
              channel,
              MetadataUtils.newAttachHeadersInterceptor(req.headers.toMetadata),
              MetadataUtils.newCaptureMetadataInterceptor(responseHeaderMetadata, responseTrailerMetadata),
            ),
            entry.methodDescriptor,
            CallOptions.DEFAULT,
            message
          )
        }).map { response =>
          val headers  = responseHeaderMetadata.get().toHeaders
          val trailers = responseTrailerMetadata.get().toHeaders

          if (logger.isTraceEnabled) {
            logger.trace(s"<<< Headers: $headers, Trailers: $trailers")
          }

          Response(Ok, headers = headers)
            .withEntity(response)
            .withTrailerHeaders(Async[F].pure(trailers))
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

        logger.warn(s"<<< Error processing request", e)
        logger.trace(s"<<< Http Status: $httpStatus, Connect Error Code: $connectCode, Message: ${rawMessage.orNull}")

        Response[F](httpStatus).withEntity(connectrpc.Error(
          code = connectCode,
          message = messageWithDetails.map(_._1),
          details = Seq.empty // details
        ))
      }
  }

  private inline def grpcMethodName(service: String, method: String): String = service + "/" + method

}
