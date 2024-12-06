package org.ivovk.connect_rpc_scala.grpc

import cats.effect.Async
import com.google.common.util.concurrent.{FutureCallback, Futures, MoreExecutors}
import io.grpc.stub.{ClientCalls, StreamObserver}
import io.grpc.{CallOptions, Channel, MethodDescriptor}

object GrpcClientCalls {

  /**
   * Asynchronous unary call.
   *
   * Optimized version of the `scalapb.grpc.ClientCalls.asyncUnaryCall` that skips Scala's Future instantiation
   * and supports cancellation.
   */
  def asyncUnaryCall[F[_] : Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    request: Req,
  ): F[Resp] = {
    Async[F].async[Resp] { cb =>
      Async[F].delay {
        val future = ClientCalls.futureUnaryCall(channel.newCall(method, options), request)

        Futures.addCallback(
          future,
          new FutureCallback[Resp] {
            def onSuccess(result: Resp): Unit = cb(Right(result))

            def onFailure(t: Throwable): Unit = cb(Left(t))
          },
          MoreExecutors.directExecutor(),
        )

        Some(Async[F].delay(future.cancel(true)))
      }
    }
  }

  /**
   * Implementation that should be faster than the [[asyncUnaryCall]].
   */
  def asyncUnaryCall2[F[_] : Async, Req, Resp](
    channel: Channel,
    method: MethodDescriptor[Req, Resp],
    options: CallOptions,
    request: Req,
  ): F[Resp] = {
    Async[F].async[Resp] { cb =>
      Async[F].delay {
        val call = channel.newCall(method, options)

        ClientCalls.asyncUnaryCall(call, request, new CallbackObserver(cb))

        Some(Async[F].delay(call.cancel("Cancelled", null)))
      }
    }
  }

  /**
   * [[StreamObserverToCallListenerAdapter]] either executes [[onNext]] -> [[onCompleted]] during the happy path
   * or just [[onError]] in case of an error.
   */
  private class CallbackObserver[F[_] : Async, Resp](cb: Either[Throwable, Resp] => Unit) extends StreamObserver[Resp] {
    private var value: Option[Either[Throwable, Resp]] = None

    override def onNext(value: Resp): Unit = {
      if this.value.isDefined then
        throw new IllegalStateException("Value already received")

      this.value = Some(Right(value))
    }

    override def onError(t: Throwable): Unit = {
      cb(Left(t))
    }

    override def onCompleted(): Unit = {
      this.value match
        case Some(v) => cb(v)
        case None => cb(Left(new IllegalStateException("No value received or call to onCompleted after onError")))
    }

  }

}
