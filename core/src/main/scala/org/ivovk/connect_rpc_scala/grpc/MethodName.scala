package org.ivovk.connect_rpc_scala.grpc

import io.grpc.MethodDescriptor

type Service = String
type Method = String

object MethodName {
  def from(descriptor: MethodDescriptor[_, _]): MethodName =
    MethodName(descriptor.getServiceName, descriptor.getBareMethodName)
}

case class MethodName(service: Service, method: Method) {
  def fullyQualifiedName: String = s"$service/$method"
}
