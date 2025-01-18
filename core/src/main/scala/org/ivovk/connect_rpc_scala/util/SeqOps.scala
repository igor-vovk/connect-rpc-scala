package org.ivovk.connect_rpc_scala.util

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object SeqOps {

  extension [A](it: Seq[A]) {
    // groupBy with preserving original ordering
    def groupByPreservingOrdering[B](f: A => B)(using ct: ClassTag[A]): Map[B, IndexedSeq[A]] = {
      val result = collection.mutable.LinkedHashMap.empty[B, ArrayBuffer[A]]

      it.foreach { elem =>
        val key = f(elem)
        result.getOrElseUpdate(key, ArrayBuffer.empty[A]) += elem
      }

      result
        .map((k, v) => (k, ArraySeq.unsafeWrapArray(v.toArray)))
        .toMap
    }
  }

  extension [A](it: IndexedSeq[A]) {
    // Returns the first element that is Some
    def colFirst[B](f: A => Option[B]): Option[B] = {
      val len = it.length
      var i   = 0
      while (i < len) {
        val x = f(it(i))
        if x.isDefined then return x
        i += 1
      }
      None
    }
  }

}
