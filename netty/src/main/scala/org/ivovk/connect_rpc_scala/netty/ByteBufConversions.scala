package org.ivovk.connect_rpc_scala.netty

import fs2.Chunk
import io.netty.buffer.{ByteBuf, ByteBufUtil, Unpooled}

// Based on
// https://github.com/typelevel/fs2-netty/blob/2cccccdfa665e92d665441ace483cdf2367cbcd6/core/src/main/scala/fs2/netty/SocketHandler.scala
object ByteBufConversions {

  def byteBufToChunk(byteBuf: ByteBuf): Chunk[Byte] =
    if (byteBuf.hasArray) {
      Chunk.array(byteBuf.array())
    } else if (byteBuf.nioBufferCount() > 0) {
      Chunk.byteBuffer(byteBuf.nioBuffer())
    } else {
      Chunk.array(ByteBufUtil.getBytes(byteBuf))
    }

  def chunkToByteBuf(chunk: Chunk[Byte]): ByteBuf =
    chunk match {
      case Chunk.ArraySlice(arr, off, len) =>
        Unpooled.wrappedBuffer(arr, off, len)
      case c: Chunk.ByteBuffer =>
        Unpooled.wrappedBuffer(c.toByteBuffer)
      case c =>
        Unpooled.wrappedBuffer(c.toArray)
    }

}
