addSbtPlugin("org.typelevel"  % "sbt-tpolecat"        % "0.5.2")
addSbtPlugin("com.github.sbt" % "sbt-ci-release"      % "1.11.2")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.7")
addSbtPlugin("com.thesamet"   % "sbt-protoc"          % "1.0.8")
addSbtPlugin("org.typelevel"  % "sbt-fs2-grpc"        % "3.0.0")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"        % "2.5.6")

libraryDependencies += "com.thesamet.scalapb"          %% "compilerplugin"   % "0.11.20"
libraryDependencies += "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.6.3"
