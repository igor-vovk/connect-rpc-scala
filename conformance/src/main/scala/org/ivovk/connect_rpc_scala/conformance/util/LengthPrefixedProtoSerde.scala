package org.ivovk.connect_rpc_scala.conformance.util

import cats.effect.Sync
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.io.{InputStream, OutputStream}

object LengthPrefixedProtoSerde {
  def systemInOut[F[_]: Sync]: LengthPrefixedProtoSerde[F] =
    new LengthPrefixedProtoSerde[F](System.in, System.out)
}

class LengthPrefixedProtoSerde[F[_]: Sync](
  in: InputStream,
  out: OutputStream,
) {
  def read[T <: Message](using comp: Companion[T]): F[T] =
    Sync[F].delay {
      val size = IntSerde.read(in)
      comp.parseFrom(in.readNBytes(size))
    }

  def write(msg: Message): F[Unit] =
    Sync[F].delay {
      IntSerde.write(out, msg.serializedSize)
      out.flush()
      out.write(msg.toByteArray)
      out.flush()
    }
}
