package me.ivovk.connect_rpc_scala.http

import connectrpc.Code

private[connect_rpc_scala] object ConnectHttpStatus {

  def fromCode(code: Code): Int = code match {
    case Code.Canceled           => 499
    case Code.InvalidArgument    => 400
    case Code.DeadlineExceeded   => 504
    case Code.NotFound           => 404
    case Code.AlreadyExists      => 409
    case Code.PermissionDenied   => 403
    case Code.ResourceExhausted  => 429
    case Code.FailedPrecondition => 400
    case Code.Aborted            => 409
    case Code.OutOfRange         => 400
    case Code.Unimplemented      => 501
    case Code.Unavailable        => 503
    case Code.Unauthenticated    => 401
    case _                       => 500
  }

  /** Infers a Connect error code when only the HTTP status is available. */
  def toCode(status: Int): Code = status match {
    case 400                   => Code.Internal
    case 401                   => Code.Unauthenticated
    case 403                   => Code.PermissionDenied
    case 404                   => Code.Unimplemented
    case 429 | 502 | 503 | 504 => Code.Unavailable
    case _                     => Code.Unknown
  }
}
