import sbt._
import Keys._

object depend {
  val scalaz    = Seq("org.scalaz"           %% "scalaz-core"     % "7.0.6",
                      "org.scalaz"           %% "scalaz-effect"   % "7.0.6")
  val specs2    = Seq("org.specs2"           %% "specs2-core",
                      "org.specs2"           %% "specs2-junit",
                      "org.specs2"           %% "specs2-html",
                      "org.specs2"           %% "specs2-matcher-extra",
                      "org.specs2"           %% "specs2-scalacheck").map(_ % "2.3.10" % "test")
  val saws      = Seq("com.ambiata"          %% "saws"            % "1.2.1-20141006225022-c52c1de" excludeAll(
    ExclusionRule(organization = "org.specs2"),
    ExclusionRule(organization = "javax.mail"),
    ExclusionRule(organization = "com.owtelse.codec"),
    ExclusionRule(organization = "com.ambiata", name = "mundane-testing_2.10")
  ))

  val mundane   = Seq("mundane-io", "mundane-control", "mundane-parse", "mundane-store").map(c =>
                      "com.ambiata"          %% c                 % MUNDANE_VERSION) ++
                  Seq("com.ambiata"          %% "mundane-testing" % MUNDANE_VERSION % "test")

  def poacher(version: String) =
    if (version.contains("cdh4"))      Seq("com.ambiata" %% "poacher" % "1.0.0-cdh4-20141006224709-49cb270")
    else if (version.contains("cdh5")) Seq("com.ambiata" %% "poacher" % "1.0.0-cdh5-20141006223408-49cb270")
    else                               sys.error(s"unsupported poacher version, can not build for $version")


  val resolvers = Seq(
    Resolver.sonatypeRepo("releases")
  , Resolver.sonatypeRepo("snapshots")
  , Resolver.sonatypeRepo("public")
  , Resolver.typesafeRepo("releases")
  , "cloudera" at "https://repository.cloudera.com/content/repositories/releases"
  , Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns)
  , "Scalaz Bintray Repo"  at "http://dl.bintray.com/scalaz/releases"
  )
}
