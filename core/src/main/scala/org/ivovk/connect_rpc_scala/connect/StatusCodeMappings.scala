package org.ivovk.connect_rpc_scala.connect

import io.grpc.Status

object StatusCodeMappings {

  private val httpStatusCodesByGrpcStatusCode: Array[Int] = {
    val maxCode = Status.Code.values.map(_.value).max
    val codes   = new Array[Int](maxCode + 1)

    Status.Code.values.foreach { code =>
      codes(code.value) = code match {
        case Status.Code.CANCELLED           => 499 // 499 Client Closed Request
        case Status.Code.UNKNOWN             => 500 // 500 Internal Server Error
        case Status.Code.INVALID_ARGUMENT    => 400 // 400 Bad Request
        case Status.Code.DEADLINE_EXCEEDED   => 504 // 504 Gateway Timeout
        case Status.Code.NOT_FOUND           => 404 // 404 Not Found
        case Status.Code.ALREADY_EXISTS      => 409 // 409 Conflict
        case Status.Code.PERMISSION_DENIED   => 403 // 403 Forbidden
        case Status.Code.RESOURCE_EXHAUSTED  => 429 // 429 Too Many Requests
        case Status.Code.FAILED_PRECONDITION => 400 // 400 Bad Request
        case Status.Code.ABORTED             => 409 // 409 Conflict
        case Status.Code.OUT_OF_RANGE        => 400 // 400 Bad Request
        case Status.Code.UNIMPLEMENTED       => 501 // 501 Not Implemented
        case Status.Code.INTERNAL            => 500 // 500 Internal Server Error
        case Status.Code.UNAVAILABLE         => 503 // 503 Service Unavailable
        case Status.Code.DATA_LOSS           => 500 // 500 Internal Server Error
        case Status.Code.UNAUTHENTICATED     => 401 // 401 Unauthorized
        case _                               => 500 // 500 Internal Server Error
      }
    }

    codes
  }

  // Used in the client to determine the gRPC status code from the HTTP status code
  // Surprisingly, not matching the previous mapping
  val GrpcStatusCodesByHttpStatusCode: Map[Int, Status.Code] = {
    val reverseMapping = Status.Code.values
      .map(code => httpStatusCodesByGrpcStatusCode(code.value) -> code)
      .toMap

    reverseMapping ++ Map(
      400 -> Status.Code.INTERNAL,      // 400 Bad Request
      404 -> Status.Code.UNIMPLEMENTED, // 404 Not Found
      409 -> Status.Code.UNKNOWN,       // 409 Conflict
      429 -> Status.Code.UNAVAILABLE,   // 429 Too Many Requests
      502 -> Status.Code.UNAVAILABLE,   // 502 Bad Gateway
      504 -> Status.Code.UNAVAILABLE,   // 504 Gateway Timeout
    )
  }

  extension (status: Status) {
    def toHttpStatusCode: Int =
      status.getCode.toHttpStatusCode

    def toConnectCode: connectrpc.Code =
      status.getCode.toConnectCode
  }

  // Url: https://connectrpc.com/docs/protocol/#error-codes
  extension (code: Status.Code) {
    def toHttpStatusCode: Int =
      httpStatusCodesByGrpcStatusCode(code.value)

    def toConnectCode: connectrpc.Code =
      connectrpc.Code.fromValue(code.value)
  }

}
