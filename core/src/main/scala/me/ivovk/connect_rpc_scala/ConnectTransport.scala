package me.ivovk.connect_rpc_scala

import cats.effect.Resource
import scalapb.GeneratedMessage

trait UnaryTransport[F[_]] {

  def unary[
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    descriptor: UnaryMethodDescriptor[Request, Response],
    request: Request,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[Response]]
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
