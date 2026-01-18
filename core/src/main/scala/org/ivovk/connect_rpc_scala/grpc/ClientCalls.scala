package org.ivovk.connect_rpc_scala.grpc

import cats.effect.{Async, Sync}
import cats.effect.std.{unsafe, Queue}
import cats.implicits.*
import fs2.Compiler.Target.forConcurrent
import fs2.Stream
import io.grpc.*

import java.util.concurrent.CountDownLatch

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
    require(
      method.getType == MethodDescriptor.MethodType.UNARY ||
        method.getType == MethodDescriptor.MethodType.CLIENT_STREAMING,
      s"Method type must be UNARY or CLIENT_STREAMING, but was ${method.getType}",
    )

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

  private type Trailers            = Metadata
  private type StreamingMessage[T] = T | Trailers

  case class StreamingResponse[F[_], T](
    headers: Metadata,
    messages: Stream[F, StreamingMessage[T]],
  )

  def serverStreamingCall[F[_]: Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Stream[F, Req],
  ): F[StreamingResponse[F, Resp]] =
    serverStreamingCall2(channel, method, options, headers, request)._2

  private def serverStreamingCall2[F[_], Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    messages: Stream[F, Req],
  )(using F: Async[F]): (ClientCall[Req, Resp], F[StreamingResponse[F, Resp]]) = {
    require(
      method.getType == MethodDescriptor.MethodType.SERVER_STREAMING,
      s"Method type must be SERVER_STREAMING, but was ${method.getType}",
    )

    val call = channel.newCall(method, options)

    val response = F.async[StreamingResponse[F, Resp]] { cb =>
      for {
        listener <- StreamingResponseListener[F, Resp](cb)
        _        <- F.delay(call.start(listener, headers))
        _        <- messages
          .evalMap(req => F.delay(call.sendMessage(req)))
          .onFinalize(F.delay(call.halfClose()))
          .compile.drain
        _ <- F.delay(call.request(Int.MaxValue))
      } yield Some(F.delay(call.cancel("Cancelled", null)))
    }

    (call, response)
  }

  private object StreamingResponseListener {

    private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

    def apply[F[_], Resp](
      cb: Either[Throwable, StreamingResponse[F, Resp]] => Unit
    )(using F: Async[F]): F[StreamingResponseListener[F, Resp]] =
      for queue <- Queue.unsafeUnbounded[F, Option[Either[Throwable, StreamingMessage[Resp]]]]
      yield new StreamingResponseListener[F, Resp](cb, queue, logger)
  }

  private class StreamingResponseListener[F[_], Resp] private (
    cb: Either[Throwable, StreamingResponse[F, Resp]] => Unit,
    queue: unsafe.UnboundedQueue[F, Option[Either[Throwable, StreamingMessage[Resp]]]],
    logger: org.slf4j.Logger,
  )(
    using F: Sync[F]
  ) extends ClientCall.Listener[Resp] {

    private val countDownLatch = new CountDownLatch(1)

    private def executeCallback(headers: Metadata): Unit = {
      if countDownLatch.getCount > 0 then countDownLatch.countDown()
      else return

      val stream = Stream.fromQueueNoneTerminated(queue).evalMap(F.fromEither)

      cb(Right(StreamingResponse(headers, stream)))
    }

    override def onHeaders(headers: Metadata): Unit = {
      logger.trace(s"<<< onHeaders: $headers")

      executeCallback(headers)
    }

    override def onMessage(message: Resp): Unit = {
      logger.trace(s"<<< onMessage: $message")

      queue.unsafeOffer(Some(Right(message)))
    }

    override def onClose(status: Status, trailers: Metadata): Unit = {
      logger.trace(s"<<< onClose: status=$status, trailers=$trailers")

      queue.unsafeOffer(Some(Either.cond(status.isOk, trailers, StatusException(status, trailers))))
      queue.unsafeOffer(None)

      executeCallback(new Metadata()) // in case onHeaders was not called
    }

  }

}
