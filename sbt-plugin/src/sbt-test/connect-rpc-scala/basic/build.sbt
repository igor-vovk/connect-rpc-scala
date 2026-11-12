ThisBuild / scalaVersion := "3.3.8"

lazy val root = project
  .in(file("."))
  .enablePlugins(ConnectRpcScalaPlugin)
