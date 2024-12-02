package org.ivovk.connect_rpc_scala.http.json

import scalapb.json4s.{AnyFormat, FormatRegistry, JsonFormat}

object ConnectJsonRegistry {

  val default: FormatRegistry = JsonFormat.DefaultRegistry
    .registerMessageFormatter[com.google.protobuf.any.Any](
      ConnectAnyFormat.anyWriter,
      AnyFormat.anyParser
    )

}
