import org.apache.spark
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, desc, expr, first, max}
import org.graphframes.GraphFrame

object Pruebas {
  def main(args: Array[String]): Unit ={

    val spark = SparkSession
      .builder
      .appName("Chess")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    /*val it = Iterator.unfold(0: Int)(s => if (s < 5) Some((s, s+1)) else None)
    while(it.hasNext) {
      println(it.next())
    }*/

    //    def unfold[A, S](init: S)(f: (S) => Option[(A, S)]): Iterator[A]
    //    Creates an Iterator that uses a function f to produce elements of
    //    type A and update an internal state of type S.

    // Vertex DataFrame
//    val v = spark.sqlContext.createDataFrame(List(
//      ("a", "Alice", 34),
//      ("b", "Bob", 36),
//      ("c", "Charlie", 30),
//      ("d", "David", 29),
//      ("e", "Esther", 32),
//      ("f", "Fanny", 36),
//      ("g", "Gabby", 60)
//    )).toDF("id", "name", "age")
//    // Edge DataFrame
//    val e = spark.sqlContext.createDataFrame(List(
//      ("a", "b", "friend"),
//      ("b", "c", "follow"),
//      ("c", "b", "follow"),
//      ("f", "c", "follow"),
//      ("e", "f", "follow"),
//      ("e", "d", "friend"),
//      ("d", "a", "friend"),
//      ("a", "e", "friend")
//    )).toDF("src", "dst", "relationship")
//    // Create a GraphFrame
//    val g = GraphFrame(v, e)
//
//    g.vertices.printSchema()
//    g.edges.printSchema()
//
//    val motif = g.find("(a)-[ab]->(b)")
//
//    g.edges.show()
//    motif.show()
//
//    println(motif.count())
//
//    println(g.edges.count())

    import org.graphframes.{examples,GraphFrame}
    val g: GraphFrame = examples.Graphs.friends  // get example graph

    // Search from "Esther" for users of age < 32.
    val paths = g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32").run()
    paths.show()

    // Specify edge filters or max path lengths.
    { g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32")
      .edgeFilter("relationship != 'friend'")
      .maxPathLength(3).run() }

    spark.sparkContext.setCheckpointDir("src\\checkpoints2")

    val result = g.connectedComponents.run() // doesn't work on Spark 1.4
    result
//      .groupBy("component")
//      .count()
      .show()

    // in Scala
    val inDeg = g.inDegrees
    inDeg.orderBy(desc("inDegree")).show(5, truncate = false)



  }

}
