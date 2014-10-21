import sbt._
import Keys._
import sbt.KeyRanks._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.ambiata.promulgate.project.ProjectPlugin._

object build extends Build {
  type Settings = Def.Setting[_]

  lazy val notion = Project(
    id = "notion"
  , base = file(".")
  , settings =
    standardSettings ++
    promulgate.library("com.ambiata.notion", "ambiata-oss")
  , aggregate =
      Seq(core)
  )
  .dependsOn(core)

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    projectSettings          ++
    compilationSettings      ++
    testingSettings


  lazy val projectSettings: Seq[Settings] = Seq(
    name := "notion"
  , version in ThisBuild := s"""0.0.1-${Option(System.getenv("HADOOP_VERSION")).getOrElse("cdh5")}"""
  , organization := "com.ambiata"
  , scalaVersion := "2.11.2"
  , crossScalaVersions := Seq(scalaVersion.value)
  , fork in run := true
  , resolvers := depend.resolvers
  )

  lazy val core = Project(
    id = "core"
    , base = file("notion-core")
    , settings = standardSettings ++ lib("core") ++ Seq[Settings](
      name := "notion-core"
    ) ++ Seq[Settings](libraryDependencies ++=
      depend.scalaz  ++
      depend.mundane ++
      depend.poacher(version.value) ++
      depend.saws    ++
      depend.specs2
    )
  )

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
  , maxErrors := 20
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

  lazy val testingSettings: Seq[Settings] = Seq(
    initialCommands in console := "import org.specs2._"
  , logBuffered := false
  , cancelable := true
  , fork in test := true
  , testOptions in Test ++= (if (Option(System.getenv("FORCE_AWS")).isDefined || Option(System.getenv("AWS_ACCESS_KEY")).isDefined)
                               Seq()
                             else
                               Seq(Tests.Argument("--", "exclude", "aws")))
  )
}
