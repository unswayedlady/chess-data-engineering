import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.graphframes.GraphFrame

object Consultas {

  def main(args: Array[String]):Unit={
    val spark = SparkSession
      .builder
      .appName("Chess")
      .master("local[*]")
      .config("spark.eventLog.enabled", value = true)
      .config("spark.eventLog.dir", "C:\\Users\\milam\\OneDrive\\Escritorio\\spark-events")
      .config("spark.history.fs.logDirectory", "C:\\Users\\milam\\OneDrive\\Escritorio\\spark-events")
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
      .json("matches.json")
      .withColumn("src", lower(expr("white.username")))// white username
      .withColumn("w_result", expr("white.result"))// white result
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
      .json("players.json")
      .withColumnRenamed("username", "id")
      .withColumn("country", substring_index(col("country"), "/", -1))

    val g = GraphFrame(v, e)

    println("1) Vertices")
    g.vertices.show()

    println("2) Edges")
    g.edges.show()
    g.edges.printSchema()

    /*g
        .edges
        .select("pgn")
        .collect()
        .foreach(println)*/

    // Basic queries

    println("1º Matches where Sicilian defense was played")

    g
      .edges
      .where(col("eco").contains("Sicilian-Defense"))
      .show(5)

    println("2º Most popular player who got max number of followers in Chess.com")

    g
      .vertices
      .groupBy(col("id"))
      .agg(max("followers").alias("max_followers"))
      .select(col("id"), col("max_followers"))
      .orderBy(desc("max_followers"))
      .show(1)

    // Motif finding

    val motif = g
      .find("(b)-[e]->(n)")


    println("3º Matches where White's player is American or Black's is Spanish")

    motif
      .filter("b.country == \"US\" OR n.country == \"ES\"")
      .select("b.id", "b.country", "n.id", "n.country")
      .filter("b.id != n.id")
      .show()

    println("4º Matches where White's player registered before 2015-09-12 00:00")

    motif
      .filter(col("b.joined") < "2015-09-12 00:00")
      .select("b.id", "b.joined")
      .distinct()
      .show()

    // Graph algorithm

    println("5º Get player with max white plays (indegree, as src is for White) and black plays (outdegree, as dst stands for Black)")

    val maxWhitePlays = g.inDegrees
    maxWhitePlays.orderBy(desc("inDegree")).show(1)

    val maxBlackPlays = g.outDegrees
    maxBlackPlays.orderBy(desc("outDegree")).show(1)

    println("6º Identify important players based on played matches")

    g
      .pageRank
      .resetProbability(0.15)
      .tol(0.01)
      .run()
      .vertices
      .distinct()
      .orderBy(desc("pagerank"))
      .show()

    println("7º Get percentage of different matches' result")

    val results =
    g
      .edges
      .groupBy(col("eco"), col("src"),
        col("dst"),col("w_result"),
        col("b_result"), col("result"))
      .count()
      .groupBy("w_result", "b_result")
      .count()
      .orderBy(desc("count"))

    results
      .withColumn("percentage", col("count") / lit(results.select(sum("count")).collect()(0).getLong(0)))
      .show()


  }

}
