package org.ivovk.connect_rpc_scala.http4s.client

import cats.effect.Sync
import cats.effect.std.Dispatcher
import cats.implicits.*
import fs2.Stream
import io.grpc.*
import org.http4s.client.Client
import org.http4s.{Header, Headers, HttpVersion, Method, Request, Uri}
import org.ivovk.connect_rpc_scala.connect.StatusCodeMappings
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders
import org.ivovk.connect_rpc_scala.grpc.MethodDescriptorExtensions.extractResponseMessageCompanionObj
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, EntityToDecode, MessageCodec}
import org.ivovk.connect_rpc_scala.http4s.Http4sHeaderMapping
import org.ivovk.connect_rpc_scala.syntax.all.*
import org.slf4j.LoggerFactory
import org.typelevel.ci.CIString
import scalapb.GeneratedMessage as Message

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}

class ConnectHttp4sChannelImpl[F[_]: Sync](
  httpClient: Client[F],
  dispatcher: Dispatcher[F],
  messageCodec: MessageCodec[F],
  headerMapping: Http4sHeaderMapping,
  baseUri: Uri,
) extends ConnectHttp4sChannel {
  private val logger = LoggerFactory.getLogger(getClass)

  private class UnaryClientCall[Req, Resp](
    md: MethodDescriptor[Req, Resp],
    callOptions: CallOptions,
  ) extends ClientCall[Req, Resp] {
    private var responseListener: ClientCall.Listener[Resp] = _
    private var headers: Metadata                           = _
    private var cancelCall: () => Future[Unit]              = () => Future.unit

    override def start(responseListener: ClientCall.Listener[Resp], headers: Metadata): Unit = {
      this.responseListener = responseListener
      this.headers = headers
    }

    override def request(numMessages: Int): Unit = {}

    override def cancel(message: String, cause: Throwable): Unit = {
      if (logger.isTraceEnabled) {
        logger.trace("Cancelling call with message: {}, cause: {}", message, cause)
      }

      Await.result(cancelCall(), Duration.Inf)
      closeCall(Status.CANCELLED.withDescription(message).withCause(cause), new Metadata())
    }

    override def halfClose(): Unit = {}

    override def sendMessage(message: Req): Unit =
      message match {
        case msg: Message =>
          cancelCall = dispatcher.unsafeRunCancelable(doSendMessage(msg))
        case _ =>
          throw new IllegalArgumentException("Message must be a generated protobuf message")
      }

    private def doSendMessage(message: Message): F[Unit] = {
      val entity = messageCodec.encode(Stream.emit(message), EncodeOptions.Default)

      val request = Request[F](
        method = Method.POST,
        uri = baseUri.addPath(md.getFullMethodName),
        headers = headerMapping.toHeaders(headers)
          .put(entity.headers.toSeq)
          .put(
            GrpcHeaders.XUserAgentKey.name -> "connect-rpc-scala-http4s"
          )
          .put(
            Option(callOptions.getDeadline).map { d =>
              Header.Raw(
                CIString(GrpcHeaders.ConnectTimeoutMsKey.name),
                d.timeRemaining(TimeUnit.MILLISECONDS).toString,
              )
            }
          ),
        body = entity.body,
      )

      httpClient.run(request)
        .use { response =>
          val metadata            = headerMapping.toMetadata(response.headers)
          val (headers, trailers) = GrpcHeaders.splitIntoHeadersAndTrailers(metadata)
          if (logger.isTraceEnabled) {
            logger.trace("<<< Response headers: {}", headers)
            logger.trace("<<< Response trailers: {}", trailers)
          }

          responseListener.onHeaders(headers)

          if (response.status.isSuccess) {
            val responseCompanion = md.extractResponseMessageCompanionObj()

            messageCodec
              .decode(EntityToDecode[F](response.body, metadata))(using responseCompanion)
              .compile
              .onlyOrError
              .attempt
              .map {
                case Left(decodeFailure) => closeCall(Status.UNKNOWN.withCause(decodeFailure), trailers)
                case Right(response)     => respondAndCloseCall(response, trailers)
              }
          } else {
            val grpcStatusByHttpStatus = StatusCodeMappings.GrpcStatusCodesByHttpStatusCode
              .get(response.status.code)
              .fold(Status.UNKNOWN)(Status.fromCode)

            messageCodec.decode[connectrpc.Error](EntityToDecode[F](response.body, metadata))
              .compile
              .onlyOrError
              .attempt
              .map {
                case Left(decodeFailure) =>
                  closeCall(grpcStatusByHttpStatus.withCause(decodeFailure), trailers)
                case Right(error) =>
                  if (logger.isTraceEnabled) {
                    logger.trace("<<< Received error response: {}", error)
                  }
                  error.details.foreach(packDetails(trailers, _))

                  val status =
                    if (error.code.isUnspecified) grpcStatusByHttpStatus
                    else Status.fromCodeValue(error.code.value)

                  closeCall(
                    status.withDescription(error.getMessage),
                    trailers,
                  )
              }
          }
        }
        .recoverWith {
          case e: TimeoutException =>
            logger.trace("Request timed out", e)
            Sync[F].delay(closeCall(Status.DEADLINE_EXCEEDED, new Metadata()))
          case e: Throwable =>
            logger.error("Error during request processing", e)
            Sync[F].delay(closeCall(Status.UNKNOWN.withCause(e), new Metadata()))
        }
    }

    private def respondAndCloseCall(message: Message, trailers: Metadata): Unit = {
      responseListener.onMessage(message.asInstanceOf[Resp])
      closeCall(Status.OK, trailers)
    }

    private def closeCall(status: Status, trailers: Metadata): Unit =
      responseListener.onClose(status, trailers)
  }

  override def toManagedChannel: ManagedChannel = ManagedChannelFacade(this)

  override def newCall[Req, Resp](
    md: MethodDescriptor[Req, Resp],
    callOptions: CallOptions,
  ): ClientCall[Req, Resp] = new UnaryClientCall[Req, Resp](md, callOptions)

  override def authority(): String =
    baseUri.authority.getOrElse(Uri.Authority()).renderString

}
