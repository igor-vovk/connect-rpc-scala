package org.ivovk.connect_rpc_scala.util

object PipeSyntax {

  extension [A](inline a: A) {
    inline def pipe[B](inline f: A => B): B = f(a)

    inline def pipeIf[B](inline cond: Boolean)(inline f: A => A): A =
      if cond then f(a)
      else a

    inline def tap[B](inline f: A => B): A = { f(a); a }

  }

}
