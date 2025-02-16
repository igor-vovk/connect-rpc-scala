package org.ivovk.connect_rpc_scala.http

object Paths {

  type Segment = String
  type Path    = List[String]

  /**
   * Decodes a path into segments and matches them against the prefix, removing the prefix segments from the
   * path.
   */
  def extractPathSegments(
    path: String,
    prefixSegments: Path = Nil,
  ): Option[Path] = {
    // TODO: optimize with manual parsing
    val pathSegments = path.stripPrefix("/").split("/").toList

    if prefixSegments eq Nil then Some(pathSegments)
    else dropPrefix(pathSegments, prefixSegments)
  }

  def dropPrefix(path: Path, prefix: Path): Option[Path] =
    var pathSegments    = path
    var unmatchedPrefix = prefix

    while !(unmatchedPrefix eq Nil) do
      if !(pathSegments eq Nil) && pathSegments.head == unmatchedPrefix.head then
        pathSegments = pathSegments.tail
        unmatchedPrefix = unmatchedPrefix.tail
      else return None

    Some(pathSegments)

}
