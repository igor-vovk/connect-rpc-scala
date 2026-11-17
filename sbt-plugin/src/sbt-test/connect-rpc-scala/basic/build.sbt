ThisBuild / scalaVersion := "3.3.9"

lazy val root = project
  .in(file("."))
  .enablePlugins(ConnectRpcScalaPlugin)
