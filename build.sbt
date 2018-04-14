name := "cj-fintech-batch"
organization := "com.cj.fintech"

version := "0.3-SNAPSHOT"

scalaVersion := "2.11.12"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5"

crossScalaVersions := Seq("2.11.12", "2.12.5")


/* PUBLISH TO MAVEN CENTRAL ***************************************************************
 *   Configuration in this section is only needed for publishing to Maven Central
 *   Repository.
 */

// `sbt release` to publish for all Scala versions
addCommandAlias("release", ";+publishSigned ;sonatypeReleaseAll")


// required project info
homepage := Some(url("https://github.com/aztecrex/scala-batch"))
scmInfo := Some(ScmInfo(url("https://github.com/aztecrex/scala-batch"),
                            "https://github.com/aztecrex/scala-batch.git"))
developers := List(Developer("aztecrex",
                             "Greg Wiley",
                             "aztecrex@jammm.com",
                             url("https://github.com/aztecrex")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

// repo layout
publishMavenStyle := true


// All CJ Engineering projects are administered through this name. Plugin needs
// so it can find the staging repositories
sonatypeProfileName := "com.cj"

// Can publish snapshot or release
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

/* END PUBLISH TO MAVEN CENTRAL ***********************************************************/
