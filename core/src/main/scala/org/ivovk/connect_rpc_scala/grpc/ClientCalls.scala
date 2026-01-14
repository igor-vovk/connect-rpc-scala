package org.ivovk.connect_rpc_scala.grpc

import cats.effect.Async
import cats.implicits.*
import fs2.Stream
import io.grpc.*

object ClientCalls {

  case class Response[T](headers: Metadata, value: T, trailers: Metadata)

  /**
   * Call that accepts one or more requests and after returns one response.
   */
  def requestStreamingCall[F[_]: Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Stream[F, Req],
  ): F[Response[Resp]] = Async[F].async { cb =>
    val call = channel.newCall(method, options)

    for
      _ <- Async[F].delay(call.start(UnaryResponseListener[Resp](cb), headers))
      _ <- request
        .evalMap(req => Async[F].delay(call.sendMessage(req)))
        .compile.drain
      resp <- Async[F].delay {
        call.halfClose()
        // request 2 messages to catch a case when a server sends more than one message
        call.request(2)

        Some(Async[F].delay(call.cancel("Cancelled", null)))
      }
    yield resp
  }

  private class UnaryResponseListener[Resp](
    cb: Either[Throwable, Response[Resp]] => Unit
  ) extends ClientCall.Listener[Resp] {

    private var headers: Option[Metadata] = None
    private var message: Option[Resp]     = None

    override def onHeaders(headers: Metadata): Unit =
      this.headers = Some(headers)

    override def onMessage(message: Resp): Unit = {
      if this.message.isDefined then throw new IllegalStateException("More than one message received")

      this.message = Some(message)
    }

    override def onClose(status: Status, trailers: Metadata): Unit = {
      val res =
        if status.isOk then
          message match
            case Some(value) =>
              Right(
                Response(
                  headers = headers.getOrElse(new Metadata()),
                  value = value,
                  trailers = trailers,
                )
              )
            case None => Left(new IllegalStateException("No value received"))
        else Left(StatusExceptionWithHeaders(status, headers.getOrElse(new Metadata()), trailers))

      cb(res)
    }

  }

}
