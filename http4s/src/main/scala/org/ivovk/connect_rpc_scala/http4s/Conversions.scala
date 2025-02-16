package org.ivovk.connect_rpc_scala.http4s

import org.ivovk.connect_rpc_scala.http.Paths

object Conversions {

  inline def http4sPathToConnectRpcPath(inline http4sPath: org.http4s.Uri.Path): Paths.Path =
    http4sPath.segments.map(_.encoded).toList

}
