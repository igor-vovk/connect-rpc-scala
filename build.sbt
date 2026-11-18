import org.typelevel.scalacoptions.ScalacOptions

ThisBuild / scalaVersion := Versions.scala3

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
    scalaVersion := {
      if (sbtVersion.value.startsWith("1.")) Versions.scala212
      else Versions.scala3ForSbt2
    },
    noPublish,
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb"   %% "compilerplugin"          % Versions.scalapb,
      "com.thesamet.scalapb"   %% "scalapb-runtime"         % Versions.scalapb,
      "org.scala-lang.modules" %% "scala-collection-compat" % Versions.scalaCollectionCompat,
      ("com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % Versions.scalapbCommonProtos)
        .excludeAll(
          ExclusionRule("com.thesamet.scalapb", "scalapb-runtime-grpc_2.12"),
          ExclusionRule("com.thesamet.scalapb", "scalapb-runtime-grpc_3"),
        ),
      "org.scalatest" %% "scalatest" % Versions.scalatest % Test,
    ),
  )

lazy val codegenPlugin = protocGenProject("protoc-gen-connect-rpc-scala", codegen)
  .settings(
    Compile / mainClass  := Some("me.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator"),
    assembly / mainClass := Some("me.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator"),
  )

lazy val sbtConnectRpcScala = project
  .in(file("sbt-plugin"))
  .enablePlugins(SbtPlugin)
  .settings(
    name         := "sbt-connect-rpc-scala",
    scalaVersion := {
      if (sbtVersion.value.startsWith("1.")) Versions.scala212
      else Versions.scala3ForSbt2
    },
    crossScalaVersions := Seq(Versions.scala212, Versions.scala3ForSbt2),
    scalacOptions ++= {
      if (scalaBinaryVersion.value == "3") Seq("-Wconf:msg=unused import:s")
      else Seq.empty
    },
    pluginCrossBuild / sbtVersion := {
      scalaBinaryVersion.value match {
        case "2.12" => Versions.sbt1Minimum
        case _      => Versions.sbt2Minimum
      }
    },
    addSbtPlugin("com.thesamet"   % "sbt-protoc"  % "1.1.0-RC2"),
    addSbtPlugin("com.github.sbt" % "sbt2-compat" % Versions.sbt2Compat),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "compilerplugin" % Versions.scalapb,
      "org.scalatest"        %% "scalatest"      % Versions.scalatest % Test,
    ),
    Compile / sourceGenerators += Def.task {
      val output =
        (Compile / sourceManaged).value /
          "me" / "ivovk" / "connect_rpc_scala" / "sbt" / "BuildInfo.scala"
      IO.write(
        output,
        s"""package me.ivovk.connect_rpc_scala.sbt
           |
           |private[sbt] object BuildInfo {
           |  val version: String = "${version.value}"
           |}
           |""".stripMargin,
      )
      Seq(output)
    }.taskValue,
    scriptedLaunchOpts += s"-Dplugin.version=${version.value}",
    scriptedSbt := {
      scalaBinaryVersion.value match {
        case "2.12" => Versions.sbt1Latest
        case _      => Versions.sbt2Latest
      }
    },
    scriptedBufferLog := false,
    scripted          := scripted
      .dependsOn(
        core / publishLocal,
        codegenPlugin.agg / publishLocal,
      )
      .evaluated,
  )

lazy val core = project
  .settings(
    name := "connect-rpc-scala-core",
    commonDeps,
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) -> (Compile / sourceManaged).value
    ),
    libraryDependencies ++= Seq(
      ("com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % Versions.scalapbCommonProtos)
        .excludeAll(
          ExclusionRule("com.thesamet.scalapb", "scalapb-runtime-grpc_3")
        ),
      "com.thesamet.scalapb"   %% "scalapb-runtime" % Versions.scalapb % "protobuf",
      "io.github.scalapb-json" %% "scalapb-circe"   % "0.17.0",
      "co.fs2"                 %% "fs2-core"        % Versions.fs2,
      "org.http4s"             %% "http4s-core"     % Versions.http4s,
    ),
  )

lazy val http4s = project
  .dependsOn(core)
  .settings(
    name := "connect-rpc-scala-http4s",
    commonDeps,
    libraryDependencies += "org.http4s" %% "http4s-client" % Versions.http4s,
  )

lazy val opentelemetry = project
  .dependsOn(core)
  .settings(
    name := "connect-rpc-scala-opentelemetry",
    commonDeps,
    libraryDependencies ++= Seq(
      "io.opentelemetry" % "opentelemetry-api"         % Versions.opentelemetry,
      "io.opentelemetry" % "opentelemetry-sdk-metrics" % Versions.opentelemetry % Test,
    ),
  )

lazy val codegenE2e = project
  .in(file("codegen-e2e"))
  .dependsOn(http4s)
  .enablePlugins(LocalCodeGenPlugin)
  .settings(
    name := "connect-rpc-scala-codegen-e2e",
    noPublish,
    commonDeps,
    libraryDependencies +=
      ("com.google.api.grpc" % "proto-google-common-protos" % Versions.googleCommonProtos % "protobuf")
        .intransitive(),
    codeGenClasspath     := (codegen / Compile / fullClasspath).value,
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) ->
        (Compile / sourceManaged).value / "scalapb",
      (genModule("me.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator$"), Seq("scala3_sources")) ->
        (Compile / sourceManaged).value / "connect",
    ),
  )

lazy val conformance = project
  .in(file("conformance/impl"))
  .dependsOn(http4s)
  .enablePlugins(LocalCodeGenPlugin, JavaAppPackaging)
  .settings(
    noPublish,
    commonDeps,
    codeGenClasspath          := (codegen / Compile / fullClasspath).value,
    Compile / PB.protoSources := Seq(baseDirectory.value.getParentFile / "protos"),
    Compile / PB.targets      := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) ->
        (Compile / sourceManaged).value / "scalapb",
      (genModule("me.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator$"), Seq("scala3_sources")) ->
        (Compile / sourceManaged).value / "connect",
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime"     % Versions.scalapb % "protobuf",
      "co.fs2"               %% "fs2-io"              % Versions.fs2,
      "co.fs2"               %% "fs2-scodec"          % Versions.fs2,
      "org.http4s"           %% "http4s-ember-server" % Versions.http4s,
      "org.http4s"           %% "http4s-ember-client" % Versions.http4s,
      "ch.qos.logback"        % "logback-classic"     % Versions.logback % Runtime,
    ),
  )

lazy val benchmarks = project
  .dependsOn(http4s)
  .enablePlugins(JmhPlugin, LocalCodeGenPlugin)
  .settings(
    name := "connect-rpc-scala-benchmarks",
    noPublish,
    codeGenClasspath     := (codegen / Compile / fullClasspath).value,
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) ->
        (Compile / sourceManaged).value / "scalapb",
      (genModule("me.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator$"), Seq("scala3_sources")) ->
        (Compile / sourceManaged).value / "connect",
    ),
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-ember-server" % Versions.http4s,
      "org.http4s" %% "http4s-ember-client" % Versions.http4s,
    ),
  )

lazy val examples = project.in(file("examples"))
  .aggregate(example_client_server, example_zio_client_server, example_interceptors)
  .settings(noPublish)

lazy val example_client_server = project.in(file("examples/client_server"))
  .dependsOn(http4s)
  .enablePlugins(LocalCodeGenPlugin, JavaAppPackaging)
  .settings(
    noPublish,
    codeGenClasspath     := (codegen / Compile / fullClasspath).value,
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) ->
        (Compile / sourceManaged).value / "scalapb",
      (genModule("me.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator$"), Seq("scala3_sources")) ->
        (Compile / sourceManaged).value / "connect",
    ),
    libraryDependencies ++= Seq(
      "me.ivovk"      %% "cedi"                % Versions.cedi,
      "org.http4s"    %% "http4s-ember-server" % Versions.http4s,
      "org.http4s"    %% "http4s-ember-client" % Versions.http4s,
      "ch.qos.logback" % "logback-classic"     % Versions.logback % Runtime,
    ),
  )

lazy val example_zio_client_server = project.in(file("examples/zio_client_server"))
  .dependsOn(http4s)
  .enablePlugins(LocalCodeGenPlugin, JavaAppPackaging)
  .settings(
    noPublish,
    codeGenClasspath     := (codegen / Compile / fullClasspath).value,
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) ->
        (Compile / sourceManaged).value / "scalapb",
      (genModule("me.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator$"), Seq("scala3_sources")) ->
        (Compile / sourceManaged).value / "connect",
    ),
    libraryDependencies ++= Seq(
      "dev.zio"       %% "zio"                 % Versions.zio,
      "dev.zio"       %% "zio-interop-cats"    % Versions.zioInteropCats,
      "org.http4s"    %% "http4s-ember-server" % Versions.http4s,
      "org.http4s"    %% "http4s-ember-client" % Versions.http4s,
      "ch.qos.logback" % "logback-classic"     % Versions.logback % Runtime,
    ),
  )

lazy val example_interceptors = project.in(file("examples/interceptors"))
  .dependsOn(http4s, opentelemetry)
  .enablePlugins(LocalCodeGenPlugin)
  .settings(
    noPublish,
    Compile / run / fork := true,
    codeGenClasspath     := (codegen / Compile / fullClasspath).value,
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) ->
        (Compile / sourceManaged).value / "scalapb",
      (genModule("me.ivovk.connect_rpc_scala.codegen.ConnectRpcScalaGenerator$"), Seq("scala3_sources")) ->
        (Compile / sourceManaged).value / "connect",
    ),
    libraryDependencies ++= Seq(
      "me.ivovk"        %% "cedi"                     % Versions.cedi,
      "io.opentelemetry" % "opentelemetry-sdk-metrics" % Versions.opentelemetry,
      "ch.qos.logback"   % "logback-classic"           % Versions.logback % Runtime,
    ),
  )

lazy val root = (project in file("."))
  .aggregate(
    codegen,
    codegenPlugin.agg,
    sbtConnectRpcScala,
    codegenE2e,
    core,
    http4s,
    opentelemetry,
    conformance,
    benchmarks,
    examples,
  )
  .settings(
    name := "connect-rpc-scala",
    noPublish,
  )
