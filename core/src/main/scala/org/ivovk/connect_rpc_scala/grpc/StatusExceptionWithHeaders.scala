package org.ivovk.connect_rpc_scala.grpc

import io.grpc.{Metadata, Status, StatusException}

class StatusExceptionWithHeaders(
  status: Status,
  headers: Metadata,
  trailers: Metadata,
) extends StatusException(status, trailers) {

  def getHeaders: Metadata = headers

}
