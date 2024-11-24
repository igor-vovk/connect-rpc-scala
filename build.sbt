ThisBuild / scalaVersion := "3.3.4"

lazy val Versions = new {
  val grpc   = "1.68.1"
  val http4s = "0.23.29"
}

lazy val core = project
  .settings(
    name := "connect-rpc-scala",

    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value
    ),

    Test / PB.targets := Seq(
      scalapb.gen() -> (Test / sourceManaged).value
    ),

    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-json4s" % "0.12.1",
      "io.grpc" % "grpc-core" % Versions.grpc,
      "io.grpc" % "grpc-protobuf" % Versions.grpc,
      "io.grpc" % "grpc-inprocess" % Versions.grpc,

      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",

      "org.http4s" %% "http4s-dsl" % Versions.http4s,
      "org.http4s" %% "http4s-client" % Versions.http4s % Test,

      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    ),
  )

lazy val `conformance` = project
  .dependsOn(core)
  .enablePlugins(Fs2Grpc, JavaAppPackaging)
  .settings(
    publish / skip := true,

    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-ember-server" % Versions.http4s,

      "ch.qos.logback" % "logback-classic" % "1.5.12" % Runtime,
    ),
  )