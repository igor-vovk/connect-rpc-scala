package org.ivovk.connect_rpc_scala

import cats.Endo
import cats.data.EitherT
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.implicits.*
import fs2.compression.Compression
import fs2.text.decodeWithCharset
import io.grpc.*
import io.grpc.MethodDescriptor.MethodType
import io.grpc.stub.MetadataUtils
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.{`Content-Encoding`, `Content-Type`}
import org.http4s.{Status, *}
import org.slf4j.{Logger, LoggerFactory}
import org.typelevel.ci.CIStringSyntax
import scalapb.grpc.ClientCalls
import scalapb.json4s.{JsonFormat, Printer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

case class Configuration(
  jsonPrinterConfiguration: Endo[Printer] = identity,
  waitForShutdown: Duration = 10.seconds,
)

object ConnectRpcHttpRoutes {

  import Converters.*


  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private given [F[_] : Async : Compression, A <: GeneratedMessage](
    using cmp: GeneratedMessageCompanion[A]
  ): EntityDecoder[F, A] with {
    override def decode(m: Media[F], strict: Boolean): DecodeResult[F, A] = {
      val charset  = m.charset.getOrElse(Charset.`UTF-8`).nioCharset
      val encoding = m.headers.get[`Content-Encoding`].map(_.contentCoding)

      val f = m.body
        .through(encoding match {
          case Some(ContentCoding.gzip) =>
            Compression[F].gunzip().andThen(_.flatMap(_.content))
          case _ =>
            identity
        })
        .through(decodeWithCharset(charset))
        .compile.string
        .flatMap { str =>
          if (logger.isTraceEnabled) {
            logger.trace(s">>> Headers: ${m.headers}")
            logger.trace(s">>> JSON: $str")
          }

          Async[F].delay(JsonFormat.fromJsonString(str))
        }

      EitherT(f.map(Right(_)))
    }

    override def consumes: Set[MediaRange] = Set(MediaRange.`application/*`)
  }

  private given [F[_] : Async, A <: GeneratedMessage](using printer: Printer): EntityEncoder[F, A] with {
    override def toEntity(a: A): Entity[F] = {
      val string = printer.print(a)

      logger.trace(s"<<< JSON: $string")

      EntityEncoder.stringEncoder[F].toEntity(string)
    }

    override val headers: Headers =
      Headers(`Content-Type`(MediaType.application.`json`))
  }

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

    given Printer = configuration.jsonPrinterConfiguration(JsonFormat.printer)

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
          methodRegistry.get(grpcMethodName(serviceName, methodName)) match {
            case Some(entry) =>
              entry.methodDescriptor.getType match
                case MethodType.UNARY =>
                  handleUnary(dsl, entry, req, ipChannel)
                case unsupported =>
                  NotImplemented(s"Unsupported method type: $unsupported")
            case None =>
              NotFound(s"Method not found: ${grpcMethodName(serviceName, methodName)}")
          }
      }

      httpApp
  }

  private def handleUnary[F[_] : Async](
    dsl: Http4sDsl[F],
    entry: ConnectRpcHttpRoutes.RegistryEntry,
    req: Request[F],
    channel: Channel
  )(using printer: Printer): F[Response[F]] = {
    import dsl.*

    req.headers.get(ci"X-Test-Case-Name") match {
      case Some(headers) =>
        logger.trace(s">>> Test case name: ${headers.head.value}")
      case None => // ignore
    }

    given GeneratedMessageCompanion[GeneratedMessage] = entry.requestMessageCompanion

    req.as[GeneratedMessage]
      .flatMap { message =>
        val responseHeaderMetadata  = new AtomicReference[Metadata]()
        val responseTrailerMetadata = new AtomicReference[Metadata]()

        logger.trace(s">>> Method: ${entry.methodDescriptor.getFullMethodName}, Entity: $message")

        Async[F].fromFuture(Async[F].delay {
          ClientCalls.asyncUnaryCall[GeneratedMessage, GeneratedMessage](
            ClientInterceptors.intercept(
              channel,
              MetadataUtils.newAttachHeadersInterceptor(http4sHeadersToGrpcMetadata(req.headers)),
              MetadataUtils.newCaptureMetadataInterceptor(responseHeaderMetadata, responseTrailerMetadata),
            ),
            entry.methodDescriptor,
            CallOptions.DEFAULT,
            message
          )
        }).flatMap { response =>
          logger.trace(s"<<< Response: $response")

          Ok(response)
            .map { resp =>
              val headers  = grpcMetadataToHttp4sHeaders(responseHeaderMetadata.get())
              val trailers = grpcMetadataToHttp4sHeaders(responseTrailerMetadata.get())
              logger.trace(s"<<< Headers: $headers, Trailers: $trailers")

              resp
                .transformHeaders(_ ++ headers)
                .withTrailerHeaders(Async[F].pure(trailers))
            }
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

        val message = e match {
          case e: StatusRuntimeException => e.getStatus.getDescription
          case e: StatusException => e.getStatus.getDescription
          case e => e.getMessage
        }

        val httpStatus  = mapGrpcStatusCodeToHttpStatus(grpcStatus.getCode)
        val connectCode = mapGrpcStatusCodeToConnectCode(grpcStatus.getCode)

        logger.warn(s"<<< Error processing request", e)
        logger.trace(s"<<< Http Status: $httpStatus, Connect Error Code: $connectCode, Message: $message")

        Response[F](httpStatus).withEntity(connectrpc.Error(
          code = connectCode,
          message = Option(message),
        ))
      }
  }

  private inline def grpcMethodName(service: String, method: String): String = service + "/" + method

}
