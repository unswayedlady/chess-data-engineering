ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "Spark-Pruebas"
  )

//val AkkaVersion = "2.6.20"

//resolvers += "SparkPackages" at "https://mvnrepository.com/repos/sonatype-releases"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
//  "org.apache.spark" %% "spark-hive" % "3.3.0",
  "com.lihaoyi" %% "requests" % "0.7.1",
  "graphframes" % "graphframes" % "0.8.1-spark3.0-s_2.12",
  "org.apache.spark" %% "spark-graphx" % "3.2.0",
  "io.spray" %%  "spray-json" % "1.3.6"
//  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
//  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test
//  "com.lihaoyi" %% "ujson" % "2.0.0"
//  "net.liftweb" %% "lift-json" % "3.5.0"

)




