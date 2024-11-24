package org.ivovk.connect_rpc_scala

import cats.data.EitherT
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.implicits.*
import fs2.compression.Compression
import fs2.text.decodeWithCharset
import io.grpc.*
import io.grpc.MethodDescriptor.MethodType
import io.grpc.stub.MetadataUtils
import org.http4s.*
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.{`Content-Encoding`, `Content-Type`}
import org.slf4j.{Logger, LoggerFactory}
import org.typelevel.ci.{CIString, CIStringSyntax}
import scalapb.grpc.ClientCalls
import scalapb.json4s.JsonFormat
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

case class Configuration(
  waitForShutdown: Duration = 10.seconds,
)

object ConnectRpcHttpRoutes {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private given [F[_] : Async : Compression, A <: GeneratedMessage](using cmp: GeneratedMessageCompanion[A]): EntityDecoder[F, A] with {
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

  private given [F[_] : Async, A <: GeneratedMessage]: EntityEncoder[F, A] with {
    override def toEntity(a: A): Entity[F] =
      EntityEncoder.stringEncoder[F].toEntity(JsonFormat.toJsonString(a))

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
  ): F[Response[F]] = {
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
              MetadataUtils.newAttachHeadersInterceptor(mkMetadata(req.headers)),
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
              val headers = mkHeaders(responseHeaderMetadata.get())
              val trailers = mkHeaders(responseTrailerMetadata.get())
              logger.trace(s"<<< Headers: $headers, Trailers: $trailers")

              resp
                .transformHeaders(_ ++ headers)
                .withTrailerHeaders(Async[F].pure(trailers))
            }
        }
      }
      .recoverWith {
        case e: StatusRuntimeException =>
          logger.error("<<< Error processing request", e)

          val description = e.getStatus.getDescription

          e.getStatus.getCode match
            case io.grpc.Status.Code.NOT_FOUND =>
              NotFound(description)
            case io.grpc.Status.Code.INVALID_ARGUMENT =>
              BadRequest(description)
            case io.grpc.Status.Code.UNIMPLEMENTED =>
              NotImplemented(description)
            case _ => // TODO: map other status codes
              InternalServerError(description)
        case e: StatusException =>
          logger.error("<<< Error processing request", e)

          InternalServerError(e.getStatus.getDescription)
        case e: Throwable =>
          logger.error("<<< Error processing request", e)

          InternalServerError(e.getMessage)
      }
  }

  private def mkMetadata(headers: Headers): Metadata = {
    val metadata = new Metadata()
    headers.foreach { header =>
      metadata.put(Metadata.Key.of(header.name.toString, Metadata.ASCII_STRING_MARSHALLER), header.value)
    }
    metadata
  }

  private def mkHeaders(metadata: Metadata): Headers = {
    val headers = metadata.keys()
      .asScala.toList
      .flatMap { key =>
        metadata.getAll(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)).asScala.map { value =>
          Header.Raw(CIString(key), value)
        }
      }

    Headers(headers)
  }

  private inline def grpcMethodName(service: String, method: String): String = service + "/" + method

}
