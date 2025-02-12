package org.ivovk.connect_rpc_scala.util

object PipeSyntax {

  extension [A](inline a: A) {
    inline def pipe[B](inline f: A => B): B = f(a)

    inline def pipeIf[B](inline cond: Boolean)(inline f: A => A): A =
      if cond then f(a)
      else a

    inline def pipeIfDefined[B](inline opt: Option[B])(inline f: (A, B) => A): A =
      opt.fold(a)(b => f(a, b))

    inline def pipeEach[B](inline iter: Iterable[B])(inline f: (A, B) => A): A =
      iter.foldLeft(a)(f)

    inline def tap[B](inline f: A => B): A = { f(a); a }
  }

}
