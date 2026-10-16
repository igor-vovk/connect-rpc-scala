package org.ivovk.connect_rpc_scala.grpc

import cats.effect.{Async, Deferred, Fiber, Sync}
import cats.effect.syntax.all.*
import cats.effect.std.{unsafe, Queue}
import cats.implicits.*
import fs2.Stream
import io.grpc.*

import java.util.concurrent.atomic.AtomicBoolean

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

  sealed trait StreamingResult[+T]
  object StreamingResult {
    case class Message[T](value: T)                                   extends StreamingResult[T]
    case class Complete(trailers: Metadata)                           extends StreamingResult[Nothing]
    case class Error(status: StatusExceptionWithHeaders)              extends StreamingResult[Nothing]
  }

  case class StreamingResponse[F[_], T](
    headers: Metadata,
    messages: Stream[F, StreamingResult[T]],
  )

  def serverStreamingCall[F[_]: Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Stream[F, Req],
  ): F[StreamingResponse[F, Resp]] =
    serverStreamingCall2(channel, method, options, headers, request)._2

  def bidiStreamingCall[F[_]: Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    request: Stream[F, Req],
  ): F[StreamingResponse[F, Resp]] =
    bidiStreamingCall2(channel, method, options, headers, request)._2

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

    streamingCall2(channel, method, options, headers, messages)
  }

  private def bidiStreamingCall2[F[_], Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    messages: Stream[F, Req],
  )(using F: Async[F]): (ClientCall[Req, Resp], F[StreamingResponse[F, Resp]]) = {
    require(
      method.getType == MethodDescriptor.MethodType.BIDI_STREAMING,
      s"Method type must be BIDI_STREAMING, but was ${method.getType}",
    )

    streamingCall2(channel, method, options, headers, messages)
  }

  private def streamingCall2[F[_], Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    headers: Metadata,
    messages: Stream[F, Req],
  )(using F: Async[F]): (ClientCall[Req, Resp], F[StreamingResponse[F, Resp]]) = {
    require(
      method.getType == MethodDescriptor.MethodType.SERVER_STREAMING ||
        method.getType == MethodDescriptor.MethodType.BIDI_STREAMING,
      s"Method type must be SERVER_STREAMING or BIDI_STREAMING, but was ${method.getType}",
    )

    val call = channel.newCall(method, options)

    val response = F.async[StreamingResponse[F, Resp]] { cb =>
      for {
        requestFiberDeferred <- Deferred[F, Fiber[F, Throwable, Unit]]
        listener             <- StreamingResponseListener[F, Resp](
          cb,
          () => call.request(1),
          requestFiberDeferred,
        )
        _        <- F.delay(call.start(listener, headers))
        _        <- F.delay(call.request(1))
        requestFiber <- messages
          .evalMap(req => F.delay {
            System.err.println(s"ClientCalls.streamingCall2: sendMessage $req")
            call.sendMessage(req)
          })
          .compile
          .drain
          .attempt
          .flatMap {
            case Right(_) =>
              F.delay {
                System.err.println("ClientCalls.streamingCall2: halfClose")
                call.halfClose()
              }
            case Left(e)  =>
              F.delay {
                System.err.println(s"ClientCalls.streamingCall2: request stream failed ${e.getMessage}")
                call.cancel("Request stream failed", e)
                listener.fail(e)
              }
          }
          .start
        _ <- requestFiberDeferred.complete(requestFiber)
      } yield Some(requestFiber.cancel *> F.delay(call.cancel("Cancelled", null)))
    }

    (call, response)
  }

  private object StreamingResponseListener {

    private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

    def apply[F[_], Resp](
      cb: Either[Throwable, StreamingResponse[F, Resp]] => Unit,
      requestNext: () => Unit,
      requestFiberDeferred: Deferred[F, Fiber[F, Throwable, Unit]],
    )(using F: Async[F]): F[StreamingResponseListener[F, Resp]] =
      for queue <- Queue.unsafeUnbounded[F, Option[Either[Throwable, StreamingResult[Resp]]]]
      yield new StreamingResponseListener[F, Resp](
        cb,
        queue,
        requestNext,
        requestFiberDeferred,
        logger,
      )
  }

  private class StreamingResponseListener[F[_], Resp] private (
    cb: Either[Throwable, StreamingResponse[F, Resp]] => Unit,
    queue: unsafe.UnboundedQueue[F, Option[Either[Throwable, StreamingResult[Resp]]]],
    requestNext: () => Unit,
    requestFiberDeferred: Deferred[F, Fiber[F, Throwable, Unit]],
    logger: org.slf4j.Logger,
  )(
    using F: Sync[F]
  ) extends ClientCall.Listener[Resp] {

    private val callbackTriggered = AtomicBoolean(false)
    private val streamCompleted   = AtomicBoolean(false)
    private var headers: Metadata = new Metadata()

    private def executeCallback(headers: Metadata): Unit = {
      if !callbackTriggered.compareAndSet(false, true) then return

      val stream = Stream.fromQueueNoneTerminated(queue)
        .evalMap(F.fromEither)
        .onFinalize(requestFiberDeferred.get.flatMap(_.cancel))

      cb(Right(StreamingResponse(headers, stream)))
    }

    private def finish(result: Either[Throwable, StreamingResult[Resp]]): Unit = {
      if !streamCompleted.compareAndSet(false, true) then return

      queue.unsafeOffer(Some(result))
      queue.unsafeOffer(None)
      executeCallback(headers)
    }

    def fail(error: Throwable): Unit =
      finish(Left(error))

    override def onHeaders(headers: Metadata): Unit = {
      logger.trace(s"<<< onHeaders: $headers")
      System.err.println(s"ClientCalls.StreamingResponseListener.onHeaders: $headers")

      this.headers = headers
      executeCallback(headers)
    }

    override def onMessage(message: Resp): Unit = {
      logger.trace(s"<<< onMessage: $message")
      System.err.println(s"ClientCalls.StreamingResponseListener.onMessage: $message")

      queue.unsafeOffer(Some(Right(StreamingResult.Message(message))))
      requestNext()
    }

    override def onClose(status: Status, trailers: Metadata): Unit = {
      logger.trace(s"<<< onClose: status=$status, trailers=$trailers")
      System.err.println(s"ClientCalls.StreamingResponseListener.onClose: status=$status trailers=$trailers")

      val terminal =
        if status.isOk then Right(StreamingResult.Complete(trailers))
        else
          Right(
            StreamingResult.Error(
              new StatusExceptionWithHeaders(status, headers, trailers)
            )
          )

      finish(terminal)
    }

  }

}
