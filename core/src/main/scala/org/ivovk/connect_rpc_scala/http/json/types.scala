package org.ivovk.connect_rpc_scala.http.json

import io.circe.Json
import scalapb_circe.{Parser, Printer}

trait Writer[T] extends ((Printer, T) => Json)

trait Reader[T] extends ((Parser, Json) => T)
