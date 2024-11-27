ThisBuild / scalaVersion := "3.3.4"

ThisBuild / organization := "io.github.igor-vovk"

ThisBuild / homepage := Some(url("https://github.com/igor-vovk/connect-rpc-scala"))
ThisBuild / licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    "igor-vovk",
    "Ihor Vovk",
    "ideals-03.gushing@icloud.com",
    url("https://ivovk.me")
  )
)
ThisBuild / sonatypeCredentialHost := xerial.sbt.Sonatype.sonatypeCentralHost

lazy val noPublish = List(
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  publish / skip := true
)

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

lazy val conformance = project
  .dependsOn(core)
  .enablePlugins(Fs2Grpc, JavaAppPackaging)
  .settings(
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-ember-server" % Versions.http4s,

      "ch.qos.logback" % "logback-classic" % "1.5.12" % Runtime,
    ),
  )

lazy val root = project
  .aggregate(
    core,
    conformance,
  )
  .settings(
    noPublish,
  )