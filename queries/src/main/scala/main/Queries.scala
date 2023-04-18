package main

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.graphframes.GraphFrame
import plotly.Plotly._
import plotly._
import scalax.chart.PieChart
import scalax.chart.module.Exporting._

object Queries {

  def main(args: Array[String]): Unit = {

    var inputMatchesPath: String = "matches.json"
    var inputPlayersPath: String = "players.json"
    var outputPath: String = "output.json"

    args.sliding(3, 3).toList.collect {
      case Array("--inputMatchesPath", inputMatches: String) => inputMatchesPath = inputMatches
      case Array("--inputPlayersPath", inputPlayers: String) => inputPlayersPath = inputPlayers
      case Array("--outputPath", output: String) => outputPath = output
    }


    val spark = SparkSession
      .builder
      .appName("Chess")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schemaMatches = StructType(Array(
      StructField("white", StructType(Array(
        StructField("username", StringType, nullable = false),
        StructField("result", StringType, nullable = false)
      ))),
      StructField("black", StructType(Array(
        StructField("username", StringType, nullable = false),
        StructField("result", StringType, nullable = false)
      ))),
      StructField("eco", StringType, nullable = true),
      StructField("pgn", StringType, nullable = false)
    ))

    val e = spark
      .read
      .schema(schemaMatches)
      .json(inputMatchesPath)
      .withColumn("src", lower(expr("white.username")))
      .withColumn("w_result", expr("white.result"))
      .withColumn("dst", lower(expr("black.username")))
      .withColumn("b_result", expr("black.result"))
      .withColumn("final_result", regexp_extract(col("pgn"), "1\\. .*(1-0|0-1|1\\/2-1\\/2)", 1))
      .withColumn("eco", substring_index(col("eco"), "/", -1))
      .withColumn("eco",  regexp_extract(col("eco"), "([^0-9]*)([0-9].*)?", 1))
      .drop("white", "black")

    val schemaProfiles = StructType(Array(
      StructField("player_id", LongType, nullable = false),
      StructField("username", StringType, nullable = false),
      StructField("title", StringType, nullable = true),
      StructField("status", StringType, nullable = false),
      StructField("country", StringType, nullable = false),
      StructField("followers", LongType, nullable = false),
      StructField("is_streamer", BooleanType, nullable = false),
      StructField("joined", TimestampType, nullable = false)
    ))

    val v = spark
      .read
      .schema(schemaProfiles)
      .json(inputPlayersPath)
      .withColumnRenamed("username", "id")
      .withColumn("country", substring_index(col("country"), "/", -1))

    val g = GraphFrame(v, e)

    // 1) Matches where Sicilian defense was played

    val q1 =
      g
        .edges
        .where(col("eco").contains("Sicilian"))
        .groupBy("eco")
        .count()
        .orderBy(desc("count"))

    // Data visualization Query 1

    val xQ1 : Seq[String] = q1.select("eco").collect().toSeq.map(_.toString().replaceAll("[\\[\\].]", "")).filterNot(_.endsWith("-"))
    val yQ1: Seq[Int] = q1.select("count").collect().toSeq.map(_.toString().replaceAll("[\\[\\]]", "").toInt)

    Bar(xQ1, yQ1, name="Q1").plot()

    q1
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(outputPath)

    // 2) Most popular player who got max number of followers in Chess.com

    g
      .vertices
      .groupBy(col("id"))
      .agg(max("followers").alias("max_followers"))
      .select(col("id"), col("max_followers"))
      .orderBy(desc("max_followers"))
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(outputPath)

    val motif = g
      .find("(w)-[e]->(b)")

    // 3) Matches where White's player is American or Black's is Spanish

    val q3 =

      motif
        .filter("w.country == \"US\" OR b.country == \"ES\"")
        .select(col("w.id").as("w_player"),
          col("w.country").as("w_country"),
          col("b.id").as("b_player"),
          col("b.country").as("b_country"))
        .filter("w_player != b_player")

    // Data visualization Query 3

    val statsUsQ3 = q3.withColumn("w_country_us", col("w_country").equalTo("US")).groupBy("w_country_us").count()

    val xWhiteUSQ3 : Seq[String] = Seq("US", "Other");
    val yWhiteUSQ3 : Seq[Double] = statsUsQ3.withColumn("percentage-white-US-people", col("count") /
      lit(statsUsQ3.select(sum("count")).collect()(0).getLong(0))).select("percentage-white-US-people").collect().toSeq.map(_.toString().replaceAll("[\\[\\]]", "").toDouble)

    Bar(xWhiteUSQ3, yWhiteUSQ3, name="White-US-Q3").plot()

    val statsEsQ3 = q3.withColumn("b_country_es", col("b_country").equalTo("ES")).groupBy("b_country_es").count()

    val xBlackESQ3 : Seq[String] = Seq("ES", "Other")
    val yBlackESQ3 = statsEsQ3.withColumn("percentage-black-ES-people", col("count") /
      lit(statsUsQ3.select(sum("count")).collect()(0).getLong(0))).select("percentage-black-ES-people").collect().toSeq.map(_.toString().replaceAll("[\\[\\]]", "").toDouble)

    Bar(xBlackESQ3, yBlackESQ3, name="Black-ES-Q3").plot()

    q3
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(outputPath)

    // 4) Matches where White's player registered before 2015-09-12 00:00

    val q4 =
      motif
        .filter(col("w.joined") < "2015-09-12 00:00")
        .select("w.id", "w.joined")
        .distinct()
        .withColumn("date", year(col("joined")))
        .groupBy("date")
        .count()

    // Data visualization Query 4

    val xQ4 : Seq[Int] = q4.select("date").collect().toSeq.map(_.toString().replaceAll("[\\[\\]]", "").toInt)
    val yQ4 : Seq[Int] = q4.select("count").collect().toSeq.map(_.toString().replaceAll("[\\[\\]]", "").toInt)

    Bar(xQ4, yQ4, name="Q4").plot()

    q4
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(outputPath)

    //5) Get player with max white plays (indegree, as src is for White) and black plays (outdegree, as dst stands for Black)

    val maxWhitePlays = g.inDegrees.orderBy(desc("inDegree"))

    maxWhitePlays
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(outputPath)

    val maxBlackPlays = g.outDegrees.orderBy(desc("outDegree"))

    maxBlackPlays
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(outputPath)

    //6) Identify important players based on played matches

    g
      .pageRank
      .resetProbability(0.15)
      .tol(0.01)
      .run()
      .vertices
      .distinct()
      .orderBy(desc("pagerank"))
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(outputPath)

    // 7) Get percentage of different matches' result

    val results =
      g
        .edges
        .groupBy("w_result", "b_result")
        .count()
        .orderBy(desc("count"))

    val q7 =
      results
        .withColumn("percentage",
          col("count") /
            lit(results.select(sum("count")).collect()(0).getLong(0)))
        .withColumn("result", concat(col("w_result"), lit("-"), col("b_result")))


    // Data visualization Query 7

    val xQ7: Seq[String] =
      q7.select("result").collect().toSeq.map(_.toString().replaceAll("[\\[\\].]", ""))
    val yQ7 : Array[Double] =
      q7.select("percentage").collect().map(_.toString().replaceAll("[\\[\\].]", "").toDouble)

    val chart = PieChart(xQ7.zip(yQ7))

    // Save to JPEG

    ChartJPEGExporter(chart).saveAsJPEG("plot-4.jpeg")

    q7
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(outputPath)

  }

}
