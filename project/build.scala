import sbt._, Keys._, KeyRanks._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.ambiata.promulgate.project.ProjectPlugin._
import scoverage.ScoverageSbtPlugin._

object build extends Build {
  type Settings = Def.Setting[_]

  lazy val notion = Project(
    id = "notion"
  , base = file(".")
  , settings =
    standardSettings ++
    promulgate.library("com.ambiata.notion", "ambiata-oss")
  , aggregate =
      Seq(core, distcopy)
  )
  .dependsOn(core, distcopy)

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    projectSettings          ++
    compilationSettings      ++
    testingSettings


  lazy val projectSettings: Seq[Settings] = Seq(
    name := "notion"
  , version in ThisBuild := s"""0.0.1-${Option(System.getenv("HADOOP_VERSION")).getOrElse("yarn")}"""
  , organization := "com.ambiata"
  , scalaVersion := "2.11.6"
  , crossScalaVersions := Seq(scalaVersion.value)
  , fork in run := true
  , resolvers := depend.resolvers
  , publishArtifact in (Test, packageBin) := true
  ) ++ Seq(prompt)

  lazy val core = Project(
    id = "core"
    , base = file("notion-core")
    , settings = standardSettings ++ lib("core") ++ Seq[Settings](
      name := "notion-core",
      addCompilerPlugin("org.spire-math" % "kind-projector_2.11" % "0.5.2")
    ) ++ Seq[Settings](libraryDependencies ++=
      depend.scalaz  ++
      depend.argonaut ++
      depend.mundane ++
      depend.origami ++
      depend.poacher(version.value) ++
      depend.saws    ++
      depend.specs2  ++
      depend.disorder
    )
  )

  lazy val distcopy = Project(
    id = "distcopy"
    , base = file("notion-distcopy")
    , settings = standardSettings ++ lib("distcopy") ++ Seq[Settings](
      name := "notion-distcopy"
    ) ++ Seq[Settings](libraryDependencies ++=
      depend.scalaz ++
      depend.saws ++
      depend.mundane ++
      depend.scalaz ++
      depend.specs2 ++
      depend.poacher(version.value) ++
      depend.hadoop(version.value) ++
      depend.disorder)
  )
  .dependsOn(core, core % "test->test")

  lazy val compilationSettings: Seq[Settings] = Seq(
    javaOptions ++= Seq(
      "-Xmx3G"
    , "-Xms512m"
    , "-Xss4m"
    )
  , javacOptions ++= Seq(
      "-source"
    , "1.6"
    , "-target"
    , "1.6"
    )
  , maxErrors := 10
  , scalacOptions ++= Seq(
      "-target:jvm-1.6"
    , "-deprecation"
    , "-unchecked"
    , "-feature"
    , "-language:_"
    , "-Ywarn-unused-import"
    , "-Ywarn-value-discard"
    , "-Yno-adapted-args"
    , "-Xlint"
    , "-Xfatal-warnings"
    , "-Yinline-warnings"
    )
  , scalacOptions in (Compile,console) := Seq("-language:_", "-feature")
  , scalacOptions in (Test,console) := Seq("-language:_", "-feature")
  )

  def lib(name: String) =
    promulgate.library(s"com.ambiata.notion.$name", "ambiata-oss")

  def app(name: String) =
    promulgate.all(s"com.ambiata.notion.$name", "ambiata-oss", "ambiata-dist")


  lazy val testingSettings: Seq[Settings] = Seq(
    initialCommands in console := "import org.specs2._"
  , logBuffered := false
  , cancelable := true
  , fork in Test := Option(System.getenv("NO_FORK")).map(_ != "true").getOrElse(true)
  , testOptions in Test += Tests.Setup(() => System.setProperty("log4j.configuration", "file:etc/log4j-test.properties"))
  , testOptions in Test ++= (if (Option(System.getenv("FORCE_AWS")).isDefined || Option(System.getenv("AWS_ACCESS_KEY")).isDefined)
                               Seq()
                             else
                               Seq(Tests.Argument("--", "exclude", "aws")))
  )

  lazy val prompt = shellPrompt in ThisBuild := { state =>
    val name = Project.extract(state).currentRef.project
    (if (name == "notion") "" else name) + "> "
  }
}
