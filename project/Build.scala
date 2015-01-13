import sbt._
import sbt.Keys._
import net.virtualvoid.sbt.graph.Plugin._
import sbtassembly.AssemblyPlugin.autoImport._

object ProjectBuild extends Build {
  val sharedSettings = Project.defaultSettings ++ graphSettings ++ Seq(
    organization := "com.tresata",
    version := "0.3.0-SNAPSHOT",
    scalaVersion := "2.10.4",
    javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.6", "-target", "1.6"),
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.6"),
    resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo",
    publishMavenStyle := true,
    pomIncludeRepository := { x => false },
    publishArtifact in Test := false
  )

  lazy val project = Project(
    id = "root",
    base = file("."),
    settings = sharedSettings ++ Seq(
      name := "spark-scalding",
      libraryDependencies ++= Seq(
        "org.slf4j" % "slf4j-api" % "1.7.5" % "compile",
        "com.twitter" %% "scalding-core" % "0.12.0" % "compile"
          exclude("com.esotericsoftware.kryo", "kryo")
          exclude("com.twitter", "chill-java")
          exclude("com.twitter", "chill_2.10"),
        "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
        "org.slf4j" % "slf4j-log4j12" % "1.7.5" % "test",
        "org.scalatest" %% "scalatest" % "2.2.1" % "test"
      )
    )
  )

  lazy val demo = Project(
    id = "demo",
    base = file("demo"),
    settings = sharedSettings ++ Seq(
      name := "spark-scalding-demo",
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.1.0" % "provided"
      ),
      assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) {
        (old) => {
          case s if s.endsWith(".class") => MergeStrategy.last
          case x => old(x)
        }
      },
      publish := { }
    )
  ).dependsOn(project)
}
