import ReleaseTransformations._

import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings

lazy val noPublish = Seq(publish := {}, publishLocal := {}, publishArtifact := false)

lazy val dagonSettings = Seq(
  organization := "com.stripe",
  scalaVersion := "2.12.3",
  crossScalaVersions := Seq("2.10.6", "2.11.11", "2.12.3"),
  libraryDependencies ++= Seq(compilerPlugin("org.spire-math" %% "kind-projector" % "0.9.4"),
                              "org.scalatest" %%% "scalatest" % "3.0.3" % Test,
                              "org.scalacheck" %%% "scalacheck" % "1.13.5" % Test),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:experimental.macros",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xfuture"
  ),
  // HACK: without these lines, the console is basically unusable,
  // since all imports are reported as being unused (and then become
  // fatal errors).
  scalacOptions in (Compile, console) ~= { _.filterNot("-Xlint" == _) },
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value,
  // release stuff
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := Function.const(false),
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
    pushChanges
  ),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("Snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("Releases" at nexus + "service/local/staging/deploy/maven2")
  },
  pomExtra := (
    <url>https://github.com/stripe/dagon</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
        <comments>A business-friendly OSS license</comments>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:stripe/dagon.git</url>
      <connection>scm:git:git@github.com:stripe/dagon.git</connection>
    </scm>
    <developers>
      <developer>
        <id>johnynek</id>
        <name>Oscar Boykin</name>
        <url>http://github.com/johnynek/</url>
      </developer>
      <developer>
        <id>non</id>
        <name>Erik Osheim</name>
        <url>http://github.com/non/</url>
      </developer>
    </developers>
  ),
  coverageMinimum := 60,
  coverageFailOnMinimum := false
) ++ mimaDefaultSettings

def previousArtifact(proj: String) =
  "com.stripe" %% s"dagon-$proj" % "0.2.4"

lazy val commonJvmSettings = Seq(
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"))

lazy val commonJsSettings = Seq(
  scalaJSStage in Global := FastOptStage,
  parallelExecution := false,
  requiresDOM := false,
  jsEnv := new org.scalajs.jsenv.nodejs.NodeJSEnv(),
  // batch mode decreases the amount of memory needed to compile scala.js code
  scalaJSOptimizerOptions := scalaJSOptimizerOptions.value.withBatchMode(
    scala.sys.env.get("TRAVIS").isDefined)
)

lazy val dagon = project
  .in(file("."))
  .settings(name := "root")
  .settings(dagonSettings: _*)
  .settings(noPublish: _*)
  .aggregate(dagonJVM, dagonJS)
  .dependsOn(dagonJVM, dagonJS)

lazy val dagonJVM = project
  .in(file(".dagonJVM"))
  .settings(moduleName := "dagon")
  .settings(dagonSettings)
  .settings(commonJvmSettings)
  .aggregate(coreJVM, benchmark)
  .dependsOn(coreJVM, benchmark)

lazy val dagonJS = project
  .in(file(".dagonJS"))
  .settings(moduleName := "dagon")
  .settings(dagonSettings)
  .settings(commonJsSettings)
  .aggregate(coreJS)
  .dependsOn(coreJS)
  .enablePlugins(ScalaJSPlugin)

lazy val core = crossProject
  .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(name := "dagon-core")
  .settings(moduleName := "dagon-core")
  .settings(dagonSettings: _*)
  .settings(mimaPreviousArtifacts := Set(previousArtifact("core")))
  .settings(libraryDependencies ++= Seq(
    "org.typelevel" %%% "catalysts-platform" % "0.8" % "test"
  ))
  .disablePlugins(JmhPlugin)
  .jsSettings(commonJsSettings: _*)
  .jsSettings(coverageEnabled := false)
  .jvmSettings(commonJvmSettings: _*)

lazy val coreJVM = core.jvm
lazy val coreJS = core.js

lazy val benchmark = project
  .in(file("benchmark"))
  .dependsOn(coreJVM)
  .settings(name := "dagon-benchmark")
  .settings(dagonSettings: _*)
  .settings(noPublish: _*)
  .settings(coverageEnabled := false)
  .enablePlugins(JmhPlugin)

lazy val docs = project
  .in(file("docs"))
  .dependsOn(coreJVM)
  .settings(name := "dagon-docs")
  .settings(dagonSettings: _*)
  .settings(noPublish: _*)
  .settings(tutSettings: _*)
  .settings(tutScalacOptions := {
    val testOptions = scalacOptions.in(test).value
    val unwantedOptions = Set("-Xlint", "-Xfatal-warnings")
    testOptions.filterNot(unwantedOptions)
  })
