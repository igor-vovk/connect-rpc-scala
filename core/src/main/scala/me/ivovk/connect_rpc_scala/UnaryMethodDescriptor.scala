package me.ivovk.connect_rpc_scala

import com.google.api.http.HttpRule
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

final case class UnaryMethodDescriptor[
  Request <: GeneratedMessage,
  Response <: GeneratedMessage,
](
  serviceName: String,
  methodName: String,
  requestCompanion: GeneratedMessageCompanion[Request],
  responseCompanion: GeneratedMessageCompanion[Response],
  httpRule: Option[HttpRule] = None,
) {
  val method: String = s"$serviceName/$methodName"
}
