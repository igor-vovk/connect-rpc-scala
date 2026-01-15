package org.ivovk.connect_rpc_scala.grpc

import cats.effect.Async
import cats.effect.std.{Dispatcher, Queue}
import cats.implicits.*
import fs2.Compiler.Target.forConcurrent
import fs2.Stream
import io.grpc.*

object ClientCalls {

  case class UnaryResponse[T](headers: Metadata, value: T, trailers: Metadata)

  /**
   * Asynchronous unary call.
   */
  def clientStreamingCall[F[_]: Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Stream[F, Req],
  ): F[UnaryResponse[Resp]] =
    clientStreamingCall2(channel, method, options, headers, request)._2

  /**
   * Asynchronous unary call with a return of the call itself.
   *
   * This method exposes the `ClientCall` object, which can be useful for advanced use cases, such as
   * cancellation or additional control over the call.
   */
  def clientStreamingCall2[F[_]: Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Stream[F, Req],
  ): (ClientCall[Req, Resp], F[UnaryResponse[Resp]]) = {
    val call = channel.newCall(method, options)

    val response = Async[F].async[UnaryResponse[Resp]] { cb =>
      for {
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
      } yield resp
    }

    (call, response)
  }

  private class UnaryResponseListener[Resp](
    cb: Either[Throwable, UnaryResponse[Resp]] => Unit
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
                UnaryResponse(
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

  object StreamingResponse {
    case class Headers(headers: Metadata)

    case class Message[T](value: T)

    case class Trailers(trailers: Metadata)

    type Compound[T] = Headers | Message[T] | Trailers
  }

  type StreamingResponse[T] = StreamingResponse.Compound[T]

  /**
   * Asynchronous unary call.
   */
  def serverStreamingCall[F[_]: Async, Req, Resp](
    channel: Channel,
    dispatcher: Dispatcher[F],
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Stream[F, Req],
  ): Stream[F, StreamingResponse[Resp]] =
    serverStreamingCall2(channel, dispatcher, method, options, headers, request)._2

  /**
   * Asynchronous unary call with a return of the call itself.
   *
   * This method exposes the `ClientCall` object, which can be useful for advanced use cases, such as
   * cancellation or additional control over the call.
   */
  private def serverStreamingCall2[F[_]: Async, Req, Resp](
    channel: Channel,
    dispatcher: Dispatcher[F],
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Stream[F, Req],
  ): (ClientCall[Req, Resp], Stream[F, StreamingResponse[Resp]]) = {
    val call = channel.newCall(method, options)

    val response = for {
      queue <- Queue.unbounded[F, Option[Either[Throwable, StreamingResponse[Resp]]]]
      _     <- Async[F].delay(call.start(StreamingResponseListener(dispatcher, queue), headers))
      _     <- request
        .evalMap(req => Async[F].delay(call.sendMessage(req)))
        .compile.drain

      resp <- Async[F].delay {
        call.halfClose()
        // request 2 messages to catch a case when a server sends more than one message
        call.request(2)

        Some(Async[F].delay(call.cancel("Cancelled", null)))
      }
    } yield Stream.fromQueueNoneTerminated(queue).evalMap(Async[F].fromEither)

    (call, Stream.eval(response).flatten)
  }

  private class StreamingResponseListener[F[_]: Async, Resp](
    dispatcher: Dispatcher[F],
    queue: Queue[F, Option[Either[Throwable, StreamingResponse[Resp]]]],
  ) extends ClientCall.Listener[Resp] {

    override def onHeaders(headers: Metadata): Unit =
      dispatcher.unsafeRunSync {
        queue.offer(Some(Right(StreamingResponse.Headers(headers))))
      }

    override def onMessage(message: Resp): Unit =
      dispatcher.unsafeRunSync {
        queue.offer(Some(Right(StreamingResponse.Message(message))))
      }

    override def onClose(status: Status, trailers: Metadata): Unit = {
      val res: Either[Exception, StreamingResponse[Resp]] =
        if status.isOk then Right(StreamingResponse.Trailers(trailers))
        else Left(StatusException(status, trailers))

      dispatcher.unsafeRunSync {
        queue.offer(Some(res)) *> queue.offer(None)
      }
    }

  }

}
