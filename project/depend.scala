import sbt._
import Keys._

object depend {
  val scalaz    = Seq("org.scalaz"           %% "scalaz-core"     % "7.1.0",
                      "org.scalaz"           %% "scalaz-effect"   % "7.1.0")

  val scopt     = Seq("com.github.scopt"     %% "scopt"           % "3.2.0")

  val specs2    = Seq("org.specs2"           %% "specs2-core",
                      "org.specs2"           %% "specs2-junit",
                      "org.specs2"           %% "specs2-html",
                      "org.specs2"           %% "specs2-matcher-extra",
                      "org.specs2"           %% "specs2-scalacheck").map(_ % "2.4.5" % "test")

  val sawsVersion = "1.2.1-20150310043654-5aa304c"
  val saws      = Seq("com.ambiata"          %% "saws-s3"            % sawsVersion excludeAll(
    ExclusionRule(organization = "org.specs2"),
    ExclusionRule(organization = "javax.mail"),
    ExclusionRule(organization = "com.owtelse.codec")
  )) ++           Seq("com.ambiata"          %% "saws-testing"       % sawsVersion % "test->test")

  val mundaneVersion = "1.2.1-20150323032355-3271ed9"
  val mundane   = Seq("mundane-io", "mundane-control", "mundane-parse", "mundane-bytes").map(c =>
                      "com.ambiata"          %% c                 % mundaneVersion) ++
                  Seq("com.ambiata"          %% "mundane-io"      % mundaneVersion % "test->test") ++
                  Seq("com.ambiata"          %% "mundane-testing" % mundaneVersion % "test")

  val disorder =  Seq("com.ambiata"          %% "disorder"        % "0.0.1-20150317050225-9c1f81e" % "test")

  def poacher(version: String) =
    if (version.contains("cdh4"))
      Seq("com.ambiata" %% "poacher" % "1.0.0-cdh4-20150310042320-6cc4adc" % "compile->compile;test->test") ++ hadoop(version)
    else if (version.contains("cdh5"))
      Seq("com.ambiata" %% "poacher" % "1.0.0-cdh5-20150310042301-6cc4adc" % "compile->compile;test->test") ++ hadoop(version)
    else
      sys.error(s"unsupported poacher version, can not build for $version")

  def hadoop(version: String, hadoopVersion: String = "2.2.0") =
    if (version.contains("cdh4"))      Seq("org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.6.0" % "provided" exclude("asm", "asm"),
                                           "org.apache.hadoop" % "hadoop-core"   % "2.0.0-mr1-cdh4.6.0" % "provided",
                                           "org.apache.avro"   % "avro-mapred"   % "1.7.4"              % "provided" classifier "hadoop2")

    else if (version.contains("cdh5")) Seq("org.apache.hadoop" % "hadoop-client" % "2.2.0-cdh5.0.0-beta-2" % "provided" exclude("asm", "asm"),
                                           "org.apache.avro"   % "avro-mapred"   % "1.7.5-cdh5.0.0-beta-2" % "provided")

    else sys.error(s"unsupported hadoop version, can not build for $version")

  val argonaut    = Seq("io.argonaut"        %% "argonaut"     % "6.1-M4")

  val resolvers = Seq(
    Resolver.sonatypeRepo("releases")
  , Resolver.sonatypeRepo("snapshots")
  , Resolver.sonatypeRepo("public")
  , Resolver.typesafeRepo("releases")
  , "cloudera" at "https://repository.cloudera.com/content/repositories/releases"
  , Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns)
  , "Scalaz Bintray Repo"  at "http://dl.bintray.com/scalaz/releases"
  , "spray.io"             at "http://repo.spray.io"
  )
}
