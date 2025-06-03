package org.ivovk.connect_rpc_scala.grpc

import cats.effect.Async
import io.grpc.*

object ClientCalls {

  case class Response[T](headers: Metadata, value: T, trailers: Metadata)

  /**
   * Asynchronous unary call.
   */
  def asyncUnaryCall[F[_]: Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Req,
  ): F[Response[Resp]] =
    asyncUnaryCall2(channel, method, options, headers, request)._2

  /**
   * Asynchronous unary call with a return of the call itself.
   *
   * This method exposes the `ClientCall` object, which can be useful for advanced use cases, such as
   * cancellation or additional control over the call.
   */
  def asyncUnaryCall2[F[_]: Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Req,
  ): (ClientCall[Req, Resp], F[Response[Resp]]) = {
    val call = channel.newCall(method, options)

    val response = Async[F].async[Response[Resp]] { cb =>
      Async[F].delay {
        call.start(CallbackListener[Resp](cb), headers)
        call.sendMessage(request)
        call.halfClose()
        // request 2 messages to catch a case when a server sends more than one message
        call.request(2)

        Some(Async[F].delay(call.cancel("Cancelled", null)))
      }
    }

    (call, response)
  }

  private class CallbackListener[Resp](
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
