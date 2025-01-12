package org.ivovk.connect_rpc_scala.grpc

import com.google.protobuf.ByteString
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

object MergingBuilder {
  extension [T <: Message](t: T) {
    def merge(other: T)(using Companion[T]): MergingBuilder[T] =
      SingleMergingBuilder(t).merge(other)
  }
}

sealed trait MergingBuilder[T <: Message] {
  def merge(other: T): MergingBuilder[T]

  def build: T
}

private class SingleMergingBuilder[T <: Message](t: T)(using cmp: Companion[T]) extends MergingBuilder[T] {
  override def merge(other: T): MergingBuilder[T] = {
    val empty = cmp.defaultInstance

    if other == empty then this
    else if t == empty then SingleMergingBuilder(other)
    else ListMergingBuilder(other :: t :: Nil)
  }

  override def build: T = t
}

private class ListMergingBuilder[T <: Message](ts: List[T])(using cmp: Companion[T]) extends MergingBuilder[T] {
  override def merge(other: T): MergingBuilder[T] = {
    val empty = cmp.defaultInstance

    if other == empty then this
    else ListMergingBuilder(other :: ts)
  }

  private def writeInReverseOrder(ts: List[T], output: ByteString.Output): Unit = {
    if (ts.nonEmpty) {
      writeInReverseOrder(ts.tail, output)

      ts.head.writeTo(output)
    }
  }

  override def build: T = {
    val output = ByteString.newOutput(ts.foldLeft(0)(_ + _.serializedSize))
    writeInReverseOrder(ts, output)
    output.close()
    cmp.parseFrom(output.toByteString.newCodedInput())
  }
}
