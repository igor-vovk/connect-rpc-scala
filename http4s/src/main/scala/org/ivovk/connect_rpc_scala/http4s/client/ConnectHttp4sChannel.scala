package org.ivovk.connect_rpc_scala.http4s.client

import io.grpc.{Channel, ManagedChannel}

trait ConnectHttp4sChannel extends Channel {

  def toManagedChannel: ManagedChannel

}
