package org.ivovk.connect_rpc_scala.grpc

import cats.effect.Async
import cats.effect.std.{unsafe, Queue}
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
  def clientStreamingCall2[F[_], Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Stream[F, Req],
  )(using F: Async[F]): (ClientCall[Req, Resp], F[UnaryResponse[Resp]]) = {
    val call = channel.newCall(method, options)

    val response = F.async[UnaryResponse[Resp]] { cb =>
      for {
        _ <- F.delay(call.start(UnaryResponseListener[Resp](cb), headers))
        _ <- request
          .evalMap(req => F.delay(call.sendMessage(req)))
          .compile.drain
        resp <- F.delay {
          call.halfClose()
          // request 2 messages to catch a case when a server sends more than one message
          call.request(2)

          Some(F.delay(call.cancel("Cancelled", null)))
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

  object StreamResponse {
    case class Headers(headers: Metadata)

    case class Message[T](value: T)

    case class Trailers(trailers: Metadata)

    type Compound[T] = Headers | Message[T] | Trailers
  }

  type StreamResponse[T] = StreamResponse.Compound[T]

  def serverStreamingCall[F[_]: Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Stream[F, Req],
  ): Stream[F, StreamResponse[Resp]] =
    serverStreamingCall2(channel, method, options, headers, request)._2

  private def serverStreamingCall2[F[_], Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Stream[F, Req],
  )(using F: Async[F]): (ClientCall[Req, Resp], Stream[F, StreamResponse[Resp]]) = {
    val call = channel.newCall(method, options)

    val responseF = for {
      queue <- Queue.unsafeUnbounded[F, Option[Either[Throwable, StreamResponse[Resp]]]]
      _     <- F.delay(call.start(StreamingResponseListener(queue), headers))
      _     <- request
        .evalMap(req => F.delay(call.sendMessage(req)))
        .onFinalize(F.delay(call.halfClose()))
        .compile.drain

      _ <- F.delay(call.request(Int.MaxValue))
    } yield Stream.fromQueueNoneTerminated(queue).evalMap(F.fromEither)

    (call, Stream.force(responseF))
  }

  private class StreamingResponseListener[F[_], Resp](
    queue: unsafe.UnboundedQueue[F, Option[Either[Throwable, StreamResponse[Resp]]]]
  ) extends ClientCall.Listener[Resp] {

    override def onHeaders(headers: Metadata): Unit =
      queue.unsafeOffer(Some(Right(StreamResponse.Headers(headers))))

    override def onMessage(message: Resp): Unit =
      queue.unsafeOffer(Some(Right(StreamResponse.Message(message))))

    override def onClose(status: Status, trailers: Metadata): Unit = {
      val res: Either[Exception, StreamResponse[Resp]] =
        if status.isOk then Right(StreamResponse.Trailers(trailers))
        else Left(StatusException(status, trailers))

      queue.unsafeOffer(Some(res))
      queue.unsafeOffer(None)
    }

  }

}
