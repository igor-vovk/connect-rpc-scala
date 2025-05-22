package org.ivovk.connect_rpc_scala.http4s.client

import cats.effect.Sync
import cats.effect.std.Dispatcher
import cats.implicits.given
import cats.{Functor, Monad, MonadThrow}
import io.grpc.*
import org.http4s.client.Client
import org.http4s.{Headers, HttpVersion, Method, Request, Uri}
import org.ivovk.connect_rpc_scala.http.RequestEntity
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

class Http4sChannel[F[_]: Sync](
  client: Client[F],
  dispatcher: Dispatcher[F],
  messageCodec: MessageCodec[F],
) extends Channel {

  class ClientCallImpl[Req <: Message, Resp <: Message]() extends ClientCall[Req, Resp] {
    private var responseListener: ClientCall.Listener[Resp] = _
    private var headers: Metadata                           = _
    private var message: Req                                = _

    override def start(responseListener: ClientCall.Listener[Resp], headers: Metadata): Unit = {
      this.responseListener = responseListener
      this.headers = headers
    }

    override def request(numMessages: Int): Unit = {}

    override def cancel(message: String, cause: Throwable): Unit = {}

    override def halfClose(): Unit = {}

    override def sendMessage(message: Req): Unit = {
      val request = Request[F](
        method = Method.POST,
        uri = Uri.unsafeFromString("/"),
        body = messageCodec.encode(message, EncodeOptions.Default).body,
      )

      val f = client.run(request).use { response =>
        val status  = response.status
        val headers = response.headers

        val metadata = new Metadata()

        responseListener.onHeaders(metadata)

        given Companion[Resp] = messageCodec.mediaType.asInstanceOf[Companion[Resp]]

        // Convert Http4s response to gRPC response
        messageCodec
          .decode[Resp](RequestEntity[F](response.body, metadata))
          .value
          .map {
            case Right(response: Resp) =>
              responseListener.onMessage(response)
            case Left(e) =>
              responseListener.onClose(Status.UNKNOWN, new Metadata());
          }
      }

      dispatcher.unsafeRunAndForget(f)
    }
  }

  override def newCall[RequestT, ResponseT](
    methodDescriptor: MethodDescriptor[RequestT, ResponseT],
    callOptions: CallOptions,
  ): ClientCall[RequestT, ResponseT] = {
    methodDescriptor.getRequestMarshaller
    new ClientCallImpl[RequestT, ResponseT]()
  }

  override def authority(): String = ???
}
