package org.ivovk.connect_rpc_scala.grpc

import cats.effect.Async
import cats.implicits.*
import io.grpc.*
import io.grpc.stub.{ClientCalls, MetadataUtils, StreamObserver}

import java.util.concurrent.atomic.AtomicReference

object GrpcClientCalls {

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
    val responseHeaderMetadata  = new AtomicReference[Metadata]()
    val responseTrailerMetadata = new AtomicReference[Metadata]()

    Async[F]
      .async[Resp] { cb =>
        Async[F].delay {
          // TODO: Interceptors to inject/capture headers are super ugly
          //  and need to be rewritten to low-level listener API
          val call = ClientInterceptors.intercept(
            channel,
            MetadataUtils.newAttachHeadersInterceptor(headers),
            MetadataUtils.newCaptureMetadataInterceptor(responseHeaderMetadata, responseTrailerMetadata),
          ).newCall(method, options)

          ClientCalls.asyncUnaryCall(call, request, CallbackListener(cb))

          Some(Async[F].delay(call.cancel("Cancelled", null)))
        }
      }
      .map { response =>
        Response(
          value = response,
          headers = responseHeaderMetadata.get,
          trailers = responseTrailerMetadata.get
        )
      }
  }

  /**
   * [[StreamObserverToCallListenerAdapter]] either executes [[onNext]] -> [[onCompleted]] during the happy path
   * or just [[onError]] in case of an error.
   *
   * During the happy path callback must be executed only after the call to [[onCompleted]],
   * otherwise response trailers are not captured.
   */
  private class CallbackListener[Resp](cb: Either[Throwable, Resp] => Unit) extends StreamObserver[Resp] {
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
