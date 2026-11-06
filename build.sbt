import org.typelevel.scalacoptions.ScalacOptions

ThisBuild / scalaVersion := "3.3.8"

ThisBuild / organization := "me.ivovk"

ThisBuild / homepage   := Some(url("https://github.com/igor-vovk/connect-rpc-scala"))
ThisBuild / licenses   := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    "igor-vovk",
    "Ihor Vovk",
    "ideals-03.gushing@icloud.com",
    url("https://ivovk.me"),
  )
)

ThisBuild / tpolecatExcludeOptions ++= Set(
  ScalacOptions.warnNonUnitStatement,
  ScalacOptions.warnValueDiscard,
)

lazy val checkNoGrpcDependencies = taskKey[Unit]("Fails when the compile dependency graph contains io.grpc artifacts")

lazy val grpcFreeDependencies = Seq(
  checkNoGrpcDependencies := {
    val grpcModules = (Compile / update).value.allModules.filter { module =>
      module.organization == "io.grpc" || module.name.startsWith("scalapb-runtime-grpc")
    }
    val log         = streams.value.log
    if (grpcModules.nonEmpty)
      sys.error(s"gRPC dependencies found: ${grpcModules.mkString(", ")}")
    else log.info("No io.grpc dependencies found")
  }
)

lazy val noPublish = List(
  publish         := {},
  publishLocal    := {},
  publishArtifact := false,
  publish / skip  := true,
)

lazy val commonDeps = Seq(
  libraryDependencies ++= Seq(
    "org.slf4j"      % "slf4j-api" % Versions.slf4j,
    "org.scalatest" %% "scalatest" % Versions.scalatest % Test,
  )
)

lazy val codegen = project
  .in(file("codegen"))
  .settings(
    name         := "connect-rpc-scala-codegen",
    scalaVersion := "2.12.20",
    noPublish,
    grpcFreeDependencies,
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb"               %% "compilerplugin"                            % Versions.scalapb,
      "com.thesamet.scalapb"               %% "scalapb-runtime"                           % Versions.scalapb,
      ("com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.78.0-1")
        .excludeAll(
          ExclusionRule("io.grpc"),
          ExclusionRule("com.thesamet.scalapb", "scalapb-runtime-grpc_2.12"),
        ),
      "org.scalatest"                      %% "scalatest"                                % Versions.scalatest % Test,
    ),
  )

lazy val codegenPlugin = protocGenProject("protoc-gen-connect-rpc-scala", codegen)
  .settings(
    Compile / mainClass := Some("org.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator"),
    assembly / mainClass := Some("org.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator"),
  )

lazy val core = project
  .settings(
    name := "connect-rpc-scala-core",
    commonDeps,
    grpcFreeDependencies,
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) -> (Compile / sourceManaged).value
    ),
    libraryDependencies ++= Seq(
      ("com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.78.0-1")
        .excludeAll(
          ExclusionRule("io.grpc"),
          ExclusionRule("com.thesamet.scalapb", "scalapb-runtime-grpc_3"),
        ),
      "com.thesamet.scalapb"               %% "scalapb-runtime"                          % Versions.scalapb % "protobuf",
      "io.github.scalapb-json"             %% "scalapb-circe"                           % "0.17.0",
      "co.fs2"                             %% "fs2-core"                                % Versions.fs2,
      "org.http4s"                         %% "http4s-core"                              % Versions.http4s,
    ),
  )

lazy val http4s = project
  .dependsOn(core)
  .settings(
    name := "connect-rpc-scala-http4s",
    grpcFreeDependencies,
    libraryDependencies += "org.http4s" %% "http4s-client" % Versions.http4s,
  )

lazy val codegenE2e = project
  .in(file("codegen-e2e"))
  .dependsOn(http4s)
  .enablePlugins(LocalCodeGenPlugin)
  .settings(
    name := "connect-rpc-scala-codegen-e2e",
    noPublish,
    commonDeps,
    grpcFreeDependencies,
    libraryDependencies +=
      ("com.google.api.grpc" % "proto-google-common-protos" % "2.78.0" % "protobuf")
        .intransitive(),
    Test / test := (Test / test).dependsOn(checkNoGrpcDependencies).value,
    codeGenClasspath := (codegen / Compile / fullClasspath).value,
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) ->
        (Compile / sourceManaged).value / "scalapb",
      (genModule("org.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator$"), Seq("scala3_sources")) ->
        (Compile / sourceManaged).value / "connect",
    ),
  )

lazy val conformance = project
  .dependsOn(http4s)
  .enablePlugins(JavaAppPackaging)
  .settings(
    noPublish,
    commonDeps,
    grpcFreeDependencies,
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) -> (Compile / sourceManaged).value
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % Versions.scalapb % "protobuf",
      "co.fs2"               %% "fs2-io"          % Versions.fs2,
      "co.fs2"               %% "fs2-scodec"      % Versions.fs2,
      "org.http4s"    %% "http4s-ember-server" % Versions.http4s,
      "org.http4s"    %% "http4s-ember-client" % Versions.http4s,
      "ch.qos.logback" % "logback-classic"     % Versions.logback % Runtime,
    ),
  )

lazy val benchmarks = project
  .dependsOn(core)
  .enablePlugins(JmhPlugin)
  .settings(
    name := "connect-rpc-scala-benchmarks",
    noPublish,
    grpcFreeDependencies,
  )

lazy val examples = project.in(file("examples"))
  .aggregate(example_client_server)
  .settings(noPublish)

lazy val example_client_server = project.in(file("examples/client_server"))
  .dependsOn(http4s)
  .enablePlugins(LocalCodeGenPlugin, JavaAppPackaging)
  .settings(
    noPublish,
    grpcFreeDependencies,
    codeGenClasspath := (codegen / Compile / fullClasspath).value,
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) ->
        (Compile / sourceManaged).value / "scalapb",
      (genModule("org.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator$"), Seq("scala3_sources")) ->
        (Compile / sourceManaged).value / "connect",
    ),
    libraryDependencies ++= Seq(
      "org.http4s"    %% "http4s-ember-server" % Versions.http4s,
      "org.http4s"    %% "http4s-ember-client" % Versions.http4s,
      "ch.qos.logback" % "logback-classic"     % Versions.logback % Runtime,
    ),
  )

lazy val root = (project in file("."))
  .aggregate(
    codegen,
    codegenPlugin.agg,
    codegenE2e,
    core,
    http4s,
    conformance,
    benchmarks,
    examples,
  )
  .settings(
    name := "connect-rpc-scala",
    noPublish,
  )
