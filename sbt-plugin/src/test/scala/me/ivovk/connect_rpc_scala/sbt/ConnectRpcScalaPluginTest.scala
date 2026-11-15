package me.ivovk.connect_rpc_scala.sbt

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import protocbridge.ProtocRunner

import java.io.File

class ConnectRpcScalaPluginTest extends AnyFlatSpec with Matchers {

  behavior of "ConnectRpcScalaPlugin.withJavaOnPath"

  it should "prepend the running JDK bin directory to the inherited PATH" in {
    val (runner, capturedEnvironment) = capturingRunner()
    val javaHome                      = new File("target/test-jdk")

    ConnectRpcScalaPlugin
      .withJavaOnPath(runner, javaHome, Some("/usr/local/bin:/usr/bin"))
      .run(Seq("--version"), Seq("LANG" -> "en_US.UTF-8"))

    capturedEnvironment() should contain theSameElementsAs Seq(
      "LANG" -> "en_US.UTF-8",
      "PATH" -> s"${new File(javaHome, "bin").getAbsolutePath}${File.pathSeparator}/usr/local/bin:/usr/bin",
    )
  }

  it should "prefer and replace an explicitly configured PATH" in {
    val (runner, capturedEnvironment) = capturingRunner()
    val javaHome                      = new File("target/test-jdk")

    ConnectRpcScalaPlugin
      .withJavaOnPath(runner, javaHome, Some("/inherited/bin"))
      .run(
        Seq("--version"),
        Seq(
          "Path" -> "/configured/bin",
          "LANG" -> "en_US.UTF-8",
        ),
      )

    capturedEnvironment() should contain theSameElementsAs Seq(
      "LANG" -> "en_US.UTF-8",
      "PATH" -> s"${new File(javaHome, "bin").getAbsolutePath}${File.pathSeparator}/configured/bin",
    )
  }

  it should "use only the JDK bin directory when no PATH is available" in {
    val (runner, capturedEnvironment) = capturingRunner()
    val javaHome                      = new File("target/test-jdk")

    ConnectRpcScalaPlugin
      .withJavaOnPath(runner, javaHome, None)
      .run(Seq("--version"), Seq.empty)

    capturedEnvironment() should contain only (
      "PATH" -> new File(javaHome, "bin").getAbsolutePath
    )
  }

  private def capturingRunner(): (ProtocRunner[Int], () => Seq[(String, String)]) = {
    var capturedEnvironment = Seq.empty[(String, String)]
    val runner              = ProtocRunner.fromFunction[Int] { (_, environment) =>
      capturedEnvironment = environment
      0
    }

    runner -> (() => capturedEnvironment)
  }
}
