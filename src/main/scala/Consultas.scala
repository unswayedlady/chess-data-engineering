import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, max}
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

    val e = spark.read.json("descarga1/part-00000-93c74744-62e4-4f4c-b332-a04cec80d7fa-c000.json")
    val v = spark.read.json("descarga2/part-00000-a1ec7dd9-80e0-41ac-b6c8-e0288502cece-c000.json")

    val g = GraphFrame(v, e)

    g.vertices.show(110)
    g.edges.show(55)

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
                  .filter("b.id != n.id")

    println("3º Matches where white player is american or black's south-african")

    motif
      .filter("b.country == \"US\" OR n.country == \"ZA\"")
      .show()

    println("4º Matches where white's player registered before 2015-09-12 00:00")

    motif
      .filter(col("b.joined") < "2015-09-12 00:00")
      .select("b.id", "b.joined")
      .distinct()
      .show()

    // Graph algorithms

    println("5º Get possible paths (depth 4) from player with premium membership to another one that has +100 followers")

    g
      .bfs
      .fromExpr("status = 'premium'")
      .toExpr("followers > 100")
      .maxPathLength(3)
      .run()
      .filter("from.id != to.id")
      .show()

    println("6º Get player with max white plays (indegree, as src is for white) and black plays (outdegree, as dst is for black)")

    val maxWhitePlays = g.inDegrees
    maxWhitePlays.orderBy(desc("inDegree")).show(1)

    val maxBlackPlays = g.outDegrees
    maxBlackPlays.orderBy(desc("outDegree")).show(1)

    println("7º Identify important players based on played matches")

    g
      .pageRank
      .resetProbability(0.15)
      .tol(0.01)
      .run()
      .vertices
      .distinct()
      .orderBy(desc("pagerank"))
      .show()


  }

}
