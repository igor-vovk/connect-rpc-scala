package org.ivovk.connect_rpc_scala.grpc

import com.google.protobuf.CodedOutputStream
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

object MergingBuilder {
  extension [T <: Message](t: T) {
    def merge(other: T)(using Companion[T]): MergingBuilder[T] =
      MergingBuilder1(t).merge(other)
  }
}

sealed trait MergingBuilder[T <: Message] {
  def merge(other: T): MergingBuilder[T]

  def build: T
}

private class MergingBuilder1[T <: Message](t: T)(using cmp: Companion[T]) extends MergingBuilder[T] {
  override def merge(other: T): MergingBuilder[T] = {
    val empty = cmp.defaultInstance

    if other == empty then this
    else if t == empty then MergingBuilder1(other)
    else MergingBuilderN(other :: t :: Nil)
  }

  override def build: T = t
}

private class MergingBuilderN[T <: Message](ts: List[T])(using cmp: Companion[T]) extends MergingBuilder[T] {
  override def merge(other: T): MergingBuilder[T] = {
    val empty = cmp.defaultInstance

    if other == empty then this
    else MergingBuilderN(other :: ts)
  }

  private case class WriteState(arr: Array[Byte], output: CodedOutputStream)

  /**
   * Iterates over the list in reverse order and writes each element to the output stream.
   *
   * Iterates over the array in one pass: Going down – counting the size of the array. Last element – creating
   * the array. Going up – writing elements to the array.
   */
  private def writeInReverseOrder(ts: List[T], size: Int = 0): WriteState =
    if (ts.isEmpty) {
      val arr    = new Array[Byte](size)
      val output = CodedOutputStream.newInstance(arr)

      WriteState(arr, output)
    } else {
      val state = writeInReverseOrder(ts.tail, size + ts.head.serializedSize)

      ts.head.writeTo(state.output)

      if (size == 0) state.output.checkNoSpaceLeft()

      state
    }

  override def build: T = {
    val arr = writeInReverseOrder(ts).arr

    cmp.parseFrom(arr)
  }
}
