package org.ivovk.connect_rpc_scala.http.codec

import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.*

/**
 * Message envelope used in streaming RPCs
 *
 * @see
 *   [[https://connectrpc.com/docs/protocol/#streaming-request Connect Protocol - Streaming]]
 */
case class EnvelopedMessage(
  isEndStream: Boolean,  // 2nd LSB (Bit 1)
  isCompressed: Boolean, // LSB (Bit 0)
  data: ByteVector,
) {
  def withEndStream(endStream: Boolean): EnvelopedMessage =
    if endStream == isEndStream then this
    else copy(isEndStream = endStream)
}

object EnvelopedMessage {

  def apply(data: ByteVector, endStream: Boolean = false): EnvelopedMessage =
    EnvelopedMessage(endStream, false, data)

  val codec: Codec[EnvelopedMessage] =
    ("reserved" | ignore(6)) ~>
      (
        ("isEndStream" | bool) ::
          ("isCompressed" | bool) ::
          ("data" | variableSizeBytesLong(uint32, bytes))
      ).as[EnvelopedMessage]
}
