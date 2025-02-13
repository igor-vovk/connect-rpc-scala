package org.ivovk.connect_rpc_scala.connect

import io.grpc.Status

object StatusCodeMappings {

  private val httpStatusCodesByGrpcStatusCode: Array[Int] = {
    val maxCode = Status.Code.values().map(_.value()).max
    val codes   = new Array[Int](maxCode + 1)

    Status.Code.values().foreach { code =>
      codes(code.value()) = code match {
        case Status.Code.CANCELLED           => 499
        case Status.Code.UNKNOWN             => 500
        case Status.Code.INVALID_ARGUMENT    => 400
        case Status.Code.DEADLINE_EXCEEDED   => 504
        case Status.Code.NOT_FOUND           => 404
        case Status.Code.ALREADY_EXISTS      => 409
        case Status.Code.PERMISSION_DENIED   => 403
        case Status.Code.RESOURCE_EXHAUSTED  => 429
        case Status.Code.FAILED_PRECONDITION => 400
        case Status.Code.ABORTED             => 409
        case Status.Code.OUT_OF_RANGE        => 400
        case Status.Code.UNIMPLEMENTED       => 501
        case Status.Code.INTERNAL            => 500
        case Status.Code.UNAVAILABLE         => 503
        case Status.Code.DATA_LOSS           => 500
        case Status.Code.UNAUTHENTICATED     => 401
        case _                               => 500
      }
    }

    codes
  }

  private val connectErrorCodeByGrpcStatusCode: Array[connectrpc.Code] = {
    val maxCode = Status.Code.values().map(_.value()).max
    val codes   = new Array[connectrpc.Code](maxCode + 1)

    Status.Code.values().foreach { code =>
      codes(code.value()) = code match {
        case Status.Code.CANCELLED           => connectrpc.Code.Canceled
        case Status.Code.UNKNOWN             => connectrpc.Code.Unknown
        case Status.Code.INVALID_ARGUMENT    => connectrpc.Code.InvalidArgument
        case Status.Code.DEADLINE_EXCEEDED   => connectrpc.Code.DeadlineExceeded
        case Status.Code.NOT_FOUND           => connectrpc.Code.NotFound
        case Status.Code.ALREADY_EXISTS      => connectrpc.Code.AlreadyExists
        case Status.Code.PERMISSION_DENIED   => connectrpc.Code.PermissionDenied
        case Status.Code.RESOURCE_EXHAUSTED  => connectrpc.Code.ResourceExhausted
        case Status.Code.FAILED_PRECONDITION => connectrpc.Code.FailedPrecondition
        case Status.Code.ABORTED             => connectrpc.Code.Aborted
        case Status.Code.OUT_OF_RANGE        => connectrpc.Code.OutOfRange
        case Status.Code.UNIMPLEMENTED       => connectrpc.Code.Unimplemented
        case Status.Code.INTERNAL            => connectrpc.Code.Internal
        case Status.Code.UNAVAILABLE         => connectrpc.Code.Unavailable
        case Status.Code.DATA_LOSS           => connectrpc.Code.DataLoss
        case Status.Code.UNAUTHENTICATED     => connectrpc.Code.Unauthenticated
        case _                               => connectrpc.Code.Internal
      }
    }

    codes
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
      httpStatusCodesByGrpcStatusCode(code.value())

    def toConnectCode: connectrpc.Code =
      connectErrorCodeByGrpcStatusCode(code.value())
  }

}
