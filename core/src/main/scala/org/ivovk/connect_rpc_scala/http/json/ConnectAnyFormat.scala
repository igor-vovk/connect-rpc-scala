package org.ivovk.connect_rpc_scala.http.json

import com.google.protobuf.any.Any as PBAny
import org.json4s.JsonAST.{JObject, JString, JValue}
import scalapb.json4s.{AnyFormat, Printer}

import java.util.Base64
import scala.language.existentials

object ConnectAnyFormat {

  private val base64enc = Base64.getEncoder.withoutPadding()

  val anyWriter: (Printer, PBAny) => JValue = { (printer, any) =>
    // Error details format in Connect protocol serialized differently from the standard spec.
    // It can be distinguished by the typeURL without the "type.googleapis.com/" prefix.
    if (!any.typeUrl.contains("/")) {
      JObject(
        "type" -> JString(any.typeUrl),
        "value" -> JString(base64enc.encodeToString(any.value.toByteArray))
      )
    } else {
      AnyFormat.anyWriter(printer, any)
    }
  }

}
