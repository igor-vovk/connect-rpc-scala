package org.ivovk.connect_rpc_scala.conformance.util

import java.io.InputStream
import java.nio.ByteBuffer

object IntSerde {
  private val IntSize = 4

  def read(in: InputStream): Int =
    ByteBuffer.wrap(in.readNBytes(IntSize)).getInt

  def write(out: java.io.OutputStream, i: Int): Unit =
    out.write(ByteBuffer.allocate(IntSize).putInt(i).array())

}
