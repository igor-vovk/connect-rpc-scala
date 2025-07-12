package org.ivovk.connect_rpc_scala.http4s.client

import io.grpc.{CallOptions, Channel, ClientCall, ManagedChannel, MethodDescriptor}

import java.util.concurrent.TimeUnit

/**
 * ZIO-gRPC requires a `ManagedChannel` to connect to a gRPC server, but ConnectHttp4sChannel abstracts away
 * the lifecycle management to http4s client. This class provides a facade for `ManagedChannel`.
 */
class ManagedChannelFacade(channel: Channel) extends ManagedChannel {

  override def newCall[RequestT, ResponseT](
    methodDescriptor: MethodDescriptor[RequestT, ResponseT],
    callOptions: CallOptions,
  ): ClientCall[RequestT, ResponseT] =
    channel.newCall(methodDescriptor, callOptions)

  override def authority(): String =
    channel.authority()

  override def shutdown(): ManagedChannel =
    this

  override def isShutdown: Boolean =
    true

  override def isTerminated: Boolean =
    true

  override def shutdownNow(): ManagedChannel =
    this

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
    true

}
