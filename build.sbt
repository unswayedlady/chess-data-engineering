ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "Spark-Pruebas"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "com.lihaoyi" %% "requests" % "0.7.1",
  "org.apache.spark" %% "spark-graphx" % "3.2.0",
  "io.spray" %%  "spray-json" % "1.3.6",
  "com.github.alexarchambault" %% "case-app" % "2.0.6",
  "com.github.alexarchambault" %% "case-app-cats" % "2.0.6"

)




