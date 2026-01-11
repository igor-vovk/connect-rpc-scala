package org.ivovk.connect_rpc_scala.http.json

import org.json4s.JsonAST.JValue
import scalapb.json4s.{Parser, Printer}

trait Writer[T] extends ((Printer, T) => JValue)

trait Reader[T] extends ((Parser, JValue) => T)
