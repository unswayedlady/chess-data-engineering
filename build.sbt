import scala.language.postfixOps

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

val sparkVersion = "3.2.0"
val caseAppVersion = "2.0.6"

lazy val root = (project in file("."))
  .settings(
    name := "Spark-Pruebas"
  )

addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.lihaoyi" %% "requests" % "0.7.1",
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "io.spray" %%  "spray-json" % "1.3.6",
  "com.github.alexarchambault" %% "case-app" % caseAppVersion,
  "com.github.alexarchambault" %% "case-app-cats" % caseAppVersion
)

assembly / mainClass := Some("Consultas")
assembly / assemblyJarName := "Consultas.jar"
assemblyMergeStrategy in assembly := {
  case path if path.contains("META-INF/services") => MergeStrategy.concat
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}




