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
      .withColumn("src", lower(expr("white.username"))) // white username
      .withColumn("w_result", expr("white.result")) // white result
      .withColumn("dst", lower(expr("black.username"))) // black username
      .withColumn("b_result", expr("black.result")) // black result
      .withColumn("final_result", regexp_extract(col("pgn"), "1\\. .*(1-0|0-1|1\\/2-1\\/2)", 1)) // final result
      .withColumn("eco", substring_index(col("eco"), "/", -1)) // el opening
      .withColumn("checkmating_piece", regexp_extract(col("pgn"), "(([1-9][0-9]*)\\. )([RQKBN]*[a-h][1-8])(#)", 0))
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

    /*println("1) Vertices")
    g.vertices.show()

    println("2) Edges")
    g.edges.show()
    g.edges.printSchema()*/

    /*g
        .edges
        .select("pgn")
        .collect()
        .foreach(println)*/

    // Basic queries

    //    println("1º Matches where Sicilian defense was played")

    g
      .edges
      .where(col("eco").contains("Sicilian-Defense"))
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(args.outputPath)

    //    println("2º Most popular player who got max number of followers in Chess.com")

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

    // Motif finding

    val motif = g
      .find("(b)-[e]->(n)")


    //    println("3º Matches where White's player is American or Black's is Spanish")

    motif
      .filter("b.country == \"US\" OR n.country == \"ES\"")
      .select(col("b.id").as("b.id"),
        col("b.country").as("b.country"),
        col("n.id").as("n.id"),
        col("n.country").as("n.country"))
      .filter("b.id != n.id")
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(args.outputPath)

    //    println("4º Matches where White's player registered before 2015-09-12 00:00")

    motif
      .filter(col("b.joined") < "2015-09-12 00:00")
      .select("b.id", "b.joined")
      .distinct()
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(args.outputPath)

    // Graph algorithm

    //    println("5º Get player with max white plays (indegree, as src is for White) and black plays (outdegree, as dst stands for Black)")

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

    //    println("6º Identify important players based on played matches")

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

    //    println("7º Get percentage of different matches' result")

    val results =
      g
        .edges
        .groupBy(col("eco"), col("src"),
          col("dst"), col("w_result"),
          col("b_result"))
        .count()
        .groupBy("w_result", "b_result")
        .count()
        .orderBy(desc("count"))

    results
      .withColumn("percentage", col("count") / lit(results.select(sum("count")).collect()(0).getLong(0)))
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(args.outputPath)

  }

}
