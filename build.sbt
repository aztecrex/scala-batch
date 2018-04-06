name := "cj-fintech-batch"
organization := "com.cj.fintech"

version := "0.1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5"

crossScalaVersions := Seq("2.11.12", "2.12.5")

homepage := Some(url("https://github.com/aztecrex/scala-batch"))
scmInfo := Some(ScmInfo(url("https://github.com/aztecrex/scala-batch"),
                            "https://github.com/aztecrex/scala-batch.git"))
developers := List(Developer("aztecrex",
                             "Greg Wiley",
                             "aztecrex@jammm.com",
                             url("https://github.com/aztecrex")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

