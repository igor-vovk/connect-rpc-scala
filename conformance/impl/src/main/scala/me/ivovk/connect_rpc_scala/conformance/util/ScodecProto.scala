package me.ivovk.connect_rpc_scala.conformance.util

import me.ivovk.connect_rpc_scala.util.PipeSyntax.*
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}
import scodec.*
import scodec.bits.ByteVector
import scodec.codecs.*

/**
 * Scodec codecs for Protobuf messages.
 */
object ScodecProto {

  def decoderFor[T <: Message](using cmp: Companion[T]): scodec.Decoder[T] =
    int32.flatMap(bytes).map(_.toArray.pipe(cmp.parseFrom))

  def encoder: scodec.Encoder[Message] =
    variableSizeBytes(int32, bytes)
      .contramap[Message](_.toByteArray.pipe(ByteVector.apply))

}
