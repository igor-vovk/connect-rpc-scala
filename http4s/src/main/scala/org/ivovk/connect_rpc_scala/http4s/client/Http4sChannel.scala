package org.ivovk.connect_rpc_scala.http4s.client

import cats.effect.Sync
import cats.effect.std.Dispatcher
import io.grpc.*
import org.http4s.client.Client
import org.http4s.{Headers, HttpVersion, Method, Request, Uri}
import org.ivovk.connect_rpc_scala.grpc.MethodDescriptorExtensions.extractResponseMessageCompanionObj
import org.ivovk.connect_rpc_scala.http.RequestEntity
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import scalapb.GeneratedMessage as Message

class Http4sChannel[F[_]: Sync](
  client: Client[F],
  dispatcher: Dispatcher[F],
  messageCodec: MessageCodec[F],
  baseUri: Uri,
) extends Channel {

  class ClientCallImpl[Req, Resp](md: MethodDescriptor[Req, Resp]) extends ClientCall[Req, Resp] {
    private var responseListener: ClientCall.Listener[Resp] = _
    private var headers: Metadata                           = _

    override def start(responseListener: ClientCall.Listener[Resp], headers: Metadata): Unit = {
      this.responseListener = responseListener
      this.headers = headers
    }

    override def request(numMessages: Int): Unit = {}

    override def cancel(message: String, cause: Throwable): Unit = {}

    override def halfClose(): Unit = {}

    override def sendMessage(message: Req): Unit =
      message match {
        case msg: Message =>
          dispatcher.unsafeRunAndForget(doSendMessage(msg))
        case _ =>
          throw new IllegalArgumentException("Message must be a generated protobuf message")
      }

    private def doSendMessage(message: Message): F[Unit] = {
      val request = Request[F](
        method = Method.POST,
        uri = baseUri.addPath(md.getFullMethodName),
        body = messageCodec.encode(message, EncodeOptions.Default).body,
      )

      client.run(request).use { response =>
        val metadata = new Metadata()

        responseListener.onHeaders(metadata)

        val responseCompanion = md.extractResponseMessageCompanionObj()

        messageCodec
          .decode(RequestEntity[F](response.body, metadata))(using responseCompanion)
          .fold(
            e => responseListener.onClose(Status.UNKNOWN.withCause(e), new Metadata()),
            response => responseListener.onMessage(response.asInstanceOf[Resp]),
          )
      }
    }
  }

  override def newCall[Req, Resp](
    md: MethodDescriptor[Req, Resp],
    callOptions: CallOptions,
  ): ClientCall[Req, Resp] = new ClientCallImpl[Req, Resp](md)

  override def authority(): String =
    baseUri.authority.getOrElse(Uri.Authority()).renderString

}
