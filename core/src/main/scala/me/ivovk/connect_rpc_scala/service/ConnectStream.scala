package me.ivovk.connect_rpc_scala.service

import cats.effect.Resource
import cats.effect.Temporal
import fs2.{Pull, Stream}
import org.http4s.Headers
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

object ConnectStream {

  final case class Output[+A] private[ConnectStream] (value: A)

  enum Event[+A] {
    case Message(value: A)
    case End(trailers: Headers)
  }

  def fromStream[F[_], A](
    messages: Stream[F, A],
    trailers: Headers = Headers.empty,
  ): ConnectStream[F, A] =
    fromPull(messages.map(Output(_)).pull.echo >> Pull.pure(trailers))

  def fromPull[F[_], A](
    pull: Pull[F, Output[A], Headers]
  ): ConnectStream[F, A] =
    new ConnectStream(pull)

  def output1[F[_], A](message: A): Pull[F, Output[A], Unit] =
    Pull.output1(Output(message))

  def done[F[_], A](trailers: Headers = Headers.empty): Pull[F, Output[A], Headers] =
    Pull.pure(trailers)

  def raiseError[F[_], A](
    error: ConnectException
  )(using fs2.RaiseThrowable[F]): ConnectStream[F, A] =
    fromPull(Pull.raiseError[F](error))

  private def fromCompletedEvents[F[_], A](
    events: Stream[F, Event[A]]
  ): ConnectStream[F, A] = {
    def go(stream: Stream[F, Event[A]]): Pull[F, Output[A], Headers] =
      stream.pull.uncons1.flatMap {
        case Some((Event.Message(value), tail)) => output1(value) >> go(tail)
        case Some((Event.End(trailers), _))     => Pull.pure(trailers)
        case None                               =>
          throw new IllegalStateException("Connect stream completed without terminal trailers")
      }

    fromPull(go(events))
  }
}

/**
 * A stream of RPC messages whose successful terminal value is the response trailers.
 *
 * Failures are raised through the pull's effect channel. In particular, a [[ConnectException]] represents a
 * Connect streaming error and carries the error trailers. Cancellation remains cancellation and does not
 * manufacture a successful terminal value.
 */
final class ConnectStream[F[_], A] private (
  private[connect_rpc_scala] val pull: Pull[F, ConnectStream.Output[A], Headers]
) {

  /**
   * Discards the successful trailers and exposes only the message stream.
   *
   * This is intended for simple clients that deliberately do not use response metadata.
   */
  def messages: Stream[F, A] =
    pull.void.stream.map(_.value)

  def map[B](f: A => B): ConnectStream[F, B] =
    ConnectStream.fromCompletedEvents(
      events.map {
        case ConnectStream.Event.Message(value) => ConnectStream.Event.Message(f(value))
        case ConnectStream.Event.End(trailers)  => ConnectStream.Event.End(trailers)
      }
    )

  def timeoutTo(
    duration: FiniteDuration,
    error: => ConnectException,
  )(using Temporal[F]): ConnectStream[F, A] =
    ConnectStream.fromCompletedEvents(
      events.timeout(duration).handleErrorWith {
        case _: TimeoutException => Stream.raiseError(error)
        case other               => Stream.raiseError(other)
      }
    )

  /**
   * Exposes messages followed by exactly one successful terminal event.
   *
   * A failed stream raises through `F` instead of emitting [[ConnectStream.Event.End]].
   */
  def events: Stream[F, ConnectStream.Event[A]] =
    pull
      .flatMap(trailers => Pull.output1(ConnectStream.Event.End(trailers)))
      .void
      .stream
      .map {
        case output: ConnectStream.Output[A] @unchecked => ConnectStream.Event.Message(output.value)
        case ConnectStream.Event.End(trailers)          => ConnectStream.Event.End(trailers)
      }
}

final case class ConnectStreamingResponse[F[_], A](
  stream: ConnectStream[F, A],
  headers: Headers = Headers.empty,
)

final case class ServerStreamingMethodDescriptor[
  Request <: GeneratedMessage,
  Response <: GeneratedMessage,
](
  serviceName: String,
  methodName: String,
  requestCompanion: GeneratedMessageCompanion[Request],
  responseCompanion: GeneratedMessageCompanion[Response],
) {
  val procedure: String = s"/$serviceName/$methodName"
}

sealed trait ServerStreamingMethod[F[_]] {
  type RequestMessage <: GeneratedMessage
  type ResponseMessage <: GeneratedMessage

  def descriptor: ServerStreamingMethodDescriptor[RequestMessage, ResponseMessage]

  def invoke(
    request: RequestMessage,
    ctx: ConnectRequestContext,
  ): Resource[F, ConnectStreamingResponse[F, ResponseMessage]]
}

object ServerStreamingMethod {

  def apply[
    F[_],
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    methodDescriptor: ServerStreamingMethodDescriptor[Request, Response],
    handler: (Request, ConnectRequestContext) => Resource[F, ConnectStreamingResponse[F, Response]],
  ): ServerStreamingMethod[F] =
    new ServerStreamingMethod[F] {
      override type RequestMessage  = Request
      override type ResponseMessage = Response

      override val descriptor: ServerStreamingMethodDescriptor[Request, Response] = methodDescriptor

      override def invoke(
        request: Request,
        ctx: ConnectRequestContext,
      ): Resource[F, ConnectStreamingResponse[F, Response]] =
        handler(request, ctx)
    }
}

trait ServerStreamingTransport[F[_]] {

  def serverStreaming[
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    descriptor: ServerStreamingMethodDescriptor[Request, Response],
    request: Request,
    ctx: ConnectRequestContext,
  ): Resource[F, ConnectStreamingResponse[F, Response]]
}

trait ConnectTransport[F[_]] extends UnaryTransport[F] with ServerStreamingTransport[F]
