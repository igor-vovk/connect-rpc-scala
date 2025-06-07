//> using scala 3.3.6
//> using jvm 21
//> using option -Xmax-inlines:64
//> using dep com.github.scopt::scopt:4.1.0
//> using dep io.circe::circe-yaml:1.15.0
//> using dep io.circe::circe-generic:0.14.13
//> using dep io.circe::circe-parser:0.14.13

/*
 * Connect RPC Scala Conformance Test Runner
 *
 * This script runs the conformance tests for Connect RPC Scala implementation.
 * It uses Docker to run the conformance tests using the same image as in the
 * GitHub Actions workflow.
 *
 * Usage:
 *   scala-cli build/conformance.sc -- [options]
 *
 * Options:
 *   -p, --profile <name>   Run a specific profile from conformance-profiles.yaml
 *   -a, --all              Run all profiles
 *   --skip-build           Skip running sbt stage before tests
 *   --stable <true|false>  Run only stable tests (default: true)
 *   -v, --verbose          Enable verbose output
 *   --docker-image <image> Docker image to use (default: ghcr.io/igor-vovk/connect-conformance-dockerimage:1.0.4-1)
 *   --logs-path <dir>      Directory to store logs (default: logs)
 *
 * Examples:
 *   # Run a specific profile
 *   scala-cli build/conformance.sc -- --profile http4s
 *
 *   # Run all profiles
 *   scala-cli build/conformance.sc -- --all
 *
 *   # Run a profile with non-stable tests
 *   scala-cli build/conformance.sc -- --profile http4s-nonstable --stable false
 */

import java.io.File
import java.nio.file.{Files, Paths, Path}
import io.circe.{yaml, Decoder}
import io.circe.generic.auto.*
import io.circe.parser
import scala.sys.process.*
import scala.util.{Try, Success, Failure}
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

case class Features(
  versions: List[String],
  protocols: List[String],
  codecs: List[String],
  stream_types: List[String],
  compressions: Option[List[String]] = None,
  supports_tls: Boolean = false,
  supports_trailers: Boolean = false,
  supports_connect_get: Boolean = true,
  supports_message_receive_limit: Option[Boolean] = None
)

case class ConformanceSpec(features: Features)

case class Profile(
  name: String,
  mode: String,
  launcher_class: String,
  conformance_spec: ConformanceSpec
)

case class Profiles(profiles: List[Profile])

// Configuration for command-line arguments
case class Config(
  profile: String = "",
  all: Boolean = false,
  skipBuild: Boolean = false,
  stable: Boolean = true,
  verbose: Boolean = false,
  dockerImage: String = "ghcr.io/igor-vovk/connect-conformance-dockerimage:1.0.4-1",
  logsPath: String = "logs",
  additionalArgs: Seq[String] = Seq()
)

object ConformanceRunner {
  private val parser = new scopt.OptionParser[Config]("conformance") {
    head("Connect RPC Scala Conformance Tests Runner")

    opt[String]('p', "profile")
      .action((x, c) => c.copy(profile = x))
      .text("Profile name to run (from conformance-profiles.yaml)")

    opt[Unit]('a', "all")
      .action((_, c) => c.copy(all = true))
      .text("Run all profiles")

    opt[Unit]("skip-build")
      .action((_, c) => c.copy(skipBuild = true))
      .text("Skip running sbt stage before tests")

    opt[Boolean]("stable")
      .action((x, c) => c.copy(stable = x))
      .text("Run only stable tests (default: true)")

    opt[Unit]('v', "verbose")
      .action((_, c) => c.copy(verbose = true))
      .text("Enable verbose output")

    opt[String]("docker-image")
      .action((x, c) => c.copy(dockerImage = x))
      .text("Docker image to use (default: ghcr.io/igor-vovk/connect-conformance-dockerimage:1.0.4-1)")

    opt[String]("logs-path")
      .action((x, c) => c.copy(logsPath = x))
      .text("Directory to store logs (default: logs)")

    arg[String]("<additional arguments>...")
      .unbounded()
      .optional()
      .action((x, c) => c.copy(additionalArgs = c.additionalArgs :+ x))
      .text("Additional arguments to pass to connectconformance")

    checkConfig(c =>
      if (c.profile.isEmpty && !c.all)
        failure("Either --profile or --all must be specified")
      else success
    )
  }

  def getBaseDir: Path = {
    // Try to find the build directory by checking current directory and parent directories
    def findBuildDir(dir: Path): Path = {
      val buildDir = dir.resolve("build")
      if (Files.exists(buildDir) && Files.isDirectory(buildDir)) {
        buildDir
      } else if (dir.getFileName.toString == "build") {
        dir
      } else {
        val parent = dir.getParent
        if (parent != null) {
          findBuildDir(parent)
        } else {
          throw new RuntimeException("Could not find build directory")
        }
      }
    }

    val currentDir = Paths.get("").toAbsolutePath
    findBuildDir(currentDir)
  }

  def ensureStageBuild(config: Config): Unit = {
    if (!config.skipBuild) {
      println("Running sbt stage...")
      val exitCode = Process("sbt stage").!
      if (exitCode != 0) {
        throw new RuntimeException("sbt stage failed!")
      }
    }
  }

  def readProfiles(): Profiles = {
    val baseDir = getBaseDir
    val profilesYaml = baseDir.resolve("conformance-profiles.yaml").toFile

    if (!profilesYaml.exists()) {
      throw new RuntimeException(s"Profiles file not found: ${profilesYaml.getAbsolutePath}")
    }

    val yamlStr = Files.readString(profilesYaml.toPath)

    import io.circe.yaml
    val result = yaml.parser.parse(yamlStr) match {
      case Right(json) => json.as[Profiles]
      case Left(error) => Left(error)
    }

    result match {
      case Right(profiles) => profiles
      case Left(error) => throw new RuntimeException(s"Failed to parse profiles: ${error.getMessage}")
    }
  }

  def runProfile(profile: Profile, config: Config): Unit = {
    println(s"Running profile: ${profile.name}")

    val baseDir = getBaseDir.toAbsolutePath
    val configFile = s"suite-${profile.name}.yaml"
    val configPath = baseDir.resolve("conformance").resolve(configFile)

    if (!Files.exists(configPath)) {
      println(s"Warning: Config file not found: ${configPath}")
      println(s"Using profile configuration directly")
    }

    // Create logs directory if it doesn't exist
    val logsDir = new File(config.logsPath)
    if (!logsDir.exists()) {
      logsDir.mkdirs()
    }

    val projectRoot = baseDir.getParent() // Get the root directory of the project

    println(s"Making sure config file is accessible: ${configFile}")

    // Construct Docker run command similar to the Dockerfile approach
    // The Docker image has /conformance/connectconformance as its entrypoint
    // We need to copy the YAML files to /conformance inside the container
    val cmd = Seq(
      "docker", "run", "--rm",
      "-v", s"${projectRoot}/conformance/target/universal/stage:/app",  // Mount the stage directory to /app
      "-v", s"${baseDir}/conformance:/conformance/suites",
      "-v", s"${logsDir.getAbsolutePath}:/logs",
      "-e", "LOGS_PATH=/logs",
      "-w", "/conformance",
      config.dockerImage,
      "--conf", s"suites/${configFile}",
      "--mode", profile.mode,
      "-v", "-vv", "--trace",
      "--",
      "/app/bin/conformance",  // Reference the script as it appears in the stage/bin directory
      "-main", s"org.ivovk.connect_rpc_scala.conformance.${profile.launcher_class}"
    ) ++ config.additionalArgs

    println(s"Running Docker command: ${cmd.mkString(" ")}")

    val exitCode = Process(cmd).!

    // Check if tests passed
    if (exitCode != 0 && config.stable) {
      throw new RuntimeException(s"Conformance tests failed for profile '${profile.name}' with exit code ${exitCode}")
    }

    println(s"Completed profile: ${profile.name} ${if (exitCode == 0) "(PASS)" else "(FAIL)"}")
  }

  def run(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(config) =>
        try {
          val profiles = readProfiles()

          // Filter profiles based on command line arguments
          val selectedProfiles = if (config.all) {
            profiles.profiles
          } else {
            profiles.profiles.filter(_.name == config.profile)
          }

          if (selectedProfiles.isEmpty) {
            println(s"No matching profiles found for: ${config.profile}")
            println("Available profiles:")
            profiles.profiles.foreach(p => println(s"  - ${p.name}"))
            System.exit(1)
          }

          // Make sure the binary is built
          ensureStageBuild(config)

          // Run each profile
          selectedProfiles.foreach { profile =>
            runProfile(profile, config)
          }

          println("All conformance tests completed successfully!")

        } catch {
          case e: Exception =>
            println(s"Error: ${e.getMessage}")
            e.printStackTrace()
            System.exit(1)
        }

      case None =>
        // Parser error, help will be shown automatically
        System.exit(1)
    }
  }
}

// For running the script directly through scala-cli
ConformanceRunner.run(args)

