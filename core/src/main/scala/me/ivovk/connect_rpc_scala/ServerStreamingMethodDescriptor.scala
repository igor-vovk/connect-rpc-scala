package me.ivovk.connect_rpc_scala

import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

final case class ServerStreamingMethodDescriptor[
  Request <: GeneratedMessage,
  Response <: GeneratedMessage,
](
  serviceName: String,
  methodName: String,
  requestCompanion: GeneratedMessageCompanion[Request],
  responseCompanion: GeneratedMessageCompanion[Response],
) {
  val method: String = s"$serviceName/$methodName"
}
