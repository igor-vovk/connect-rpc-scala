package org.ivovk.connect_rpc_scala.http.codec

import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.*

case class EnvelopedMessage(
  reserved: Int,         // 6 bits (MSB)
  isEndStream: Boolean,  // 2nd LSB (Bit 1)
  isCompressed: Boolean, // LSB (Bit 0)
  data: ByteVector,
)

object EnvelopedMessage {
  val codec: Codec[EnvelopedMessage] = {
    ("reserved" | uint(6)) ::
      ("isEndStream" | bool) ::
      ("isCompressed" | bool) ::
      ("data" | variableSizeBytesLong(uint32, bytes))
  }.as[EnvelopedMessage]
}
