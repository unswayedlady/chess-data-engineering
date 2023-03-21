package main

import caseapp.{CaseApp, RemainingArgs}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.graphframes.GraphFrame

object Queries extends CaseApp[ArgumentsQueries] {

  def run(args: ArgumentsQueries, r: RemainingArgs): Unit = {
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
      .json(args.inputMatchesPath)
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
      .json(args.inputPlayersPath)
      .withColumnRenamed("username", "id")
      .withColumn("country", substring_index(col("country"), "/", -1))

    val g = GraphFrame(v, e)

    val q1 =
      g
        .edges
        .where(col("eco").contains("Sicilian"))
        .groupBy("eco")
        .count()
        .orderBy(desc("count"))

    import plotly._, Plotly._

    //visualización de la consulta 1

    val x : Seq[String] = q1.select("eco").collect().toSeq.map(_.toString().replaceAll("[\\[\\].]", "")).filterNot(_.endsWith("-"))
    val y: Seq[Int] = q1.select("count").collect().toSeq.map(_.toString().replaceAll("[\\[\\]]", "").toInt)

    Bar(x, y, name="q1").plot()

    q1
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(args.outputPath)

    g
      .vertices
      .groupBy(col("id"))
      .agg(max("followers").alias("max_followers"))
      .select(col("id"), col("max_followers"))
      .orderBy(desc("max_followers"))
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(args.outputPath)

    val motif = g
      .find("(w)-[e]->(b)")

      motif
        .filter("w.country == \"US\" OR b.country == \"ES\"")
        .select(col("w.id").as("w_player"),
          col("w.country").as("w_country"),
          col("b.id").as("b_player"),
          col("b.country").as("b_country"))
        .filter("w_player != b_player") .coalesce(1)
        .write
        .mode(SaveMode.Append)
        .json(args.outputPath)

    motif
      .filter(col("w.joined") < "2015-09-12 00:00")
      .select("w.id", "w.joined")
      .distinct()
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(args.outputPath)

    val maxWhitePlays = g.inDegrees
    maxWhitePlays.orderBy(desc("inDegree"))
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(args.outputPath)

    val maxBlackPlays = g.outDegrees
    maxBlackPlays.orderBy(desc("outDegree"))
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(args.outputPath)

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
      .json(args.outputPath)

    val results =
      g
        .edges
        .groupBy("w_result", "b_result")
        .count()
        .orderBy(desc("count"))

    results
      .withColumn("percentage",
        col("count") /
          lit(results.select(sum("count")).collect()(0).getLong(0)))
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(args.outputPath)

  }

}
