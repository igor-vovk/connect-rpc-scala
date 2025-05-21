package org.ivovk.connect_rpc_scala.conformance.util

import cats.effect.Sync
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.io.{InputStream, OutputStream}

object ProtoSerDeser {
  def read[F[_]: Sync, T <: Message](in: InputStream)(using comp: Companion[T]): F[T] =
    Sync[F].delay {
      val size = IntSerDeser.read(in)
      comp.parseFrom(in.readNBytes(size))
    }

  def write[F[_]: Sync](out: OutputStream, msg: Message): F[Unit] =
    Sync[F].delay {
      IntSerDeser.write(out, msg.serializedSize)
      out.flush()
      out.write(msg.toByteArray)
      out.flush()
    }
}
