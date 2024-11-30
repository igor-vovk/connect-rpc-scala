package org.ivovk.connect_rpc_scala.http

import org.http4s.{Header, ParseResult}
import org.typelevel.ci.CIStringSyntax

import scala.annotation.targetName

object Headers {

  @targetName("ConnectTimeoutMs")
  case class `Connect-Timeout-Ms`(value: Long)

  @targetName("ConnectTimeoutMs$")
  object `Connect-Timeout-Ms` {
    def parse(s: String): ParseResult[`Connect-Timeout-Ms`] = {
      ParseResult.fromTryCatchNonFatal(s)(`Connect-Timeout-Ms`(s.toLong))
    }

    implicit val header: Header[`Connect-Timeout-Ms`, Header.Single] = Header.createRendered(
      ci"Connect-Timeout-Ms",
      _.value,
      parse
    )
  }

  @targetName("XTestCaseName")
  case class `X-Test-Case-Name`(value: String)

  @targetName("XTestCaseName$")
  object `X-Test-Case-Name` {
    @targetName("HeaderXTestCaseName")
    implicit val header: Header[`X-Test-Case-Name`, Header.Single] = Header.createRendered(
      ci"X-Test-Case-Name",
      _.value,
      v => ParseResult.success(`X-Test-Case-Name`(v))
    )
  }

}
