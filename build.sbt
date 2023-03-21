import scala.language.postfixOps

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"
val caseAppVersion = "2.0.0-M3"
val catsVersion = "2.0.0-M2"


lazy val root = (project in file("."))
  .settings(
    name := "SparkProject"
  )
  .aggregate(
    download,
    queries
  )

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq("com.github.alexarchambault" %% "case-app" % caseAppVersion)
)

lazy val assemblySettings = Seq(
  assembly / mainClass := Some("src/main/scala/queries/main/Queries.scala"),
  assembly / assemblyJarName := "Queries.jar",
  assemblyMergeStrategy in assembly := {
    case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
    case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
    case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case "git.properties" => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
}
)

lazy val download = project
  .settings(
    commonSettings,
    name += "Download",
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
    libraryDependencies ++= Seq(
      "com.lihaoyi" %% "requests" % "0.7.1",
      "io.spray" %%  "spray-json" % "1.3.6",
      "org.typelevel" %% "cats-effect" % catsVersion
    )
  )

lazy val queries = project
  .settings(
    commonSettings,
    name += "Queries",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-graphx" % sparkVersion,
      "org.plotly-scala" %% "plotly-render" % "0.7.0"
    ),
    assemblySettings
  )