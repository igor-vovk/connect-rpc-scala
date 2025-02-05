package org.ivovk.connect_rpc_scala.conformance.util

import cats.effect.Sync
import connectrpc.conformance.v1.{ServerCompatRequest, ServerCompatResponse}

import java.io.InputStream

object ServerCompatSerDeser {
  def readRequest[F[_]: Sync](
    in: InputStream
  ): F[ServerCompatRequest] = Sync[F].delay {
    val requestSize = IntSerDeser.read(in)

    ServerCompatRequest.parseFrom(in.readNBytes(requestSize))
  }

  def writeResponse[F[_]: Sync](
    out: java.io.OutputStream,
    resp: ServerCompatResponse,
  ): F[Unit] = Sync[F].delay {
    IntSerDeser.write(out, resp.serializedSize)
    out.flush()
    out.write(resp.toByteArray)
    out.flush()
  }
}
