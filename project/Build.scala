import sbt._
import sbt.Keys._
import net.virtualvoid.sbt.graph.Plugin._
import sbtassembly.AssemblyPlugin.autoImport._

object ProjectBuild extends Build {
  val sharedSettings = Project.defaultSettings ++ graphSettings ++ Seq(
    organization := "com.tresata",
    version := "0.6.0-SNAPSHOT",
    scalaVersion := "2.10.4",
    crossScalaVersions := Seq("2.10.4", "2.11.7"),
    javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.6", "-target", "1.6"),
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.6"),
    resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo",
    resolvers += "Sonatype Public" at "https://oss.sonatype.org/content/groups/public/",
    publishMavenStyle := true,
    pomIncludeRepository := { x => false },
    publishArtifact in Test := false,
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },
    credentials += Credentials(Path.userHome / ".m2" / "credentials_sonatype"),
    pomExtra := (
      <url>https://github.com/tresata/spark-scalding</url>
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>  
          <distribution>repo</distribution>
          <comments>A business-friendly OSS license</comments>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:tresata/spark-scalding.git</url>
        <connection>scm:git:git@github.com:tresata/spark-scalding.git</connection>
      </scm>
      <developers>
        <developer>
          <id>koertkuipers</id>
          <name>Koert Kuipers</name>
          <url>https://github.com/koertkuipers</url>
        </developer>
      </developers>)
  )

  lazy val project = Project(
    id = "root",
    base = file("."),
    settings = sharedSettings ++ Seq(
      name := "spark-scalding",
      libraryDependencies ++= Seq(
        "org.slf4j" % "slf4j-api" % "1.7.5" % "compile",
        "com.twitter" %% "scalding-core" % "0.13.1" % "compile"
          exclude("com.esotericsoftware.kryo", "kryo")
          exclude("com.twitter", "chill-java")
          exclude("com.twitter", "chill_2.10")
          exclude("com.twitter", "chill_2.11"),
        "com.tresata" %% "spark-sorted" % "0.3.1" % "compile",
        "org.apache.spark" %% "spark-core" % "1.5.0" % "provided",
        "org.slf4j" % "slf4j-log4j12" % "1.7.5" % "test",
        "org.scalatest" %% "scalatest" % "2.2.5" % "test"
      )
    )
  )

  lazy val demo = Project(
    id = "demo",
    base = file("demo"),
    settings = sharedSettings ++ Seq(
      name := "spark-scalding-demo",
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"
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
