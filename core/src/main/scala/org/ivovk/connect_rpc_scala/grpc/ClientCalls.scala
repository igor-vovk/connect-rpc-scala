package org.ivovk.connect_rpc_scala.grpc

import cats.effect.Async
import io.grpc.*

object ClientCalls {

  case class Response[T](value: T, headers: Metadata, trailers: Metadata)

  /**
   * Asynchronous unary call.
   */
  def asyncUnaryCall[F[_] : Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Req,
  ): F[Response[Resp]] = {
    Async[F].async[Response[Resp]] { cb =>
      Async[F].delay {
        val call = channel.newCall(method, options)
        call.start(CallbackListener[Resp](cb), headers)
        call.sendMessage(request)
        call.halfClose()
        call.request(2)

        Some(Async[F].delay(call.cancel("Cancelled", null)))
      }
    }
  }

  private class CallbackListener[Resp](cb: Either[Throwable, Response[Resp]] => Unit) extends ClientCall.Listener[Resp] {
    private var headers: Option[Metadata] = None
    private var message: Option[Resp]     = None

    override def onHeaders(headers: Metadata): Unit = {
      this.headers = Some(headers)
    }

    override def onMessage(message: Resp): Unit = {
      if this.message.isDefined then
        throw new IllegalStateException("More than one message received")

      this.message = Some(message)
    }

    override def onClose(status: Status, trailers: Metadata): Unit = {
      if status.isOk then
        message match
          case Some(value) => cb(Right(Response(
            value = value,
            headers = headers.getOrElse(new Metadata()),
            trailers = trailers
          )))
          case None => cb(Left(new IllegalStateException("No value received")))
      else
        cb(Left(status.asException(trailers)))
    }
  }

}
