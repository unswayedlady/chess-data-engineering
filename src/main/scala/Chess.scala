import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.omg.CORBA.portable.UnknownException
import requests.{InvalidCertException, RequestFailedException}


object Chess {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Capítulo10")
      .master("local[*]")
      .config("spark.network.timeout", "10000s")
      .config("spark.executor.heartbeatInterval", "5000s")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    var titledPlayersJson = Seq[String]()
    val titles = Seq("GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM")


    titles.foreach(title => {
      try{
        titledPlayersJson = requests.get("https://api.chess.com/pub/titled/" + title).text() +: titledPlayersJson
      }catch{
        case _ : requests.TimeoutException => null
        case _ : UnknownException => null
        case _ : RequestFailedException => null
        case _ : InvalidCertException => null
      }
    })

    import spark.implicits._

    var titledPlayersJsonDf = spark
      .read
      .json(titledPlayersJson.toDS())
      .selectExpr("explode(players) as players")
      .limit(50)

    // -> Vertex

    // Profile player

    def getProfilePlayerInfo(playerName: String) : String = {
      try{
        requests.get("https://api.chess.com/pub/player/" + playerName).text()
      }catch{
        case _ : requests.TimeoutException => null
        case _ : UnknownException => null
        case _ : RequestFailedException => null
        case _ : InvalidCertException =>
          null
      }
    }
    val getProfilePlayerInfoUdf = udf(getProfilePlayerInfo(_:String) : String)

    val profilePlayerSchema = new StructType(Array(
      StructField("username", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("joined", TimestampType, nullable = true),
      StructField("last_online", TimestampType, nullable = true),
      StructField("followers", LongType, nullable = true),
      StructField("is_streamer", BooleanType, nullable = true),
      StructField("verified", BooleanType, nullable = true),
      StructField("twitch_url", BooleanType, nullable = true),
      StructField("fide", LongType, nullable = true),
    ))

    titledPlayersJsonDf = titledPlayersJsonDf
      .select(from_json(getProfilePlayerInfoUdf(col("players")), profilePlayerSchema)
        .alias("profile_player"))
      .na
      .drop()


    // Player stats

    def getPlayerStatsInfo(playerName: String) : String = {
      try{
        requests.get("https://api.chess.com/pub/player/" + playerName + "/stats").text()
      }catch{
        case _ : requests.TimeoutException => null
        case _ : UnknownException => null
        case _ : RequestFailedException => null
        case _ : InvalidCertException => null
      }

    }

    val getPlayerStatsInfoUdf = udf(getPlayerStatsInfo(_:String) : String)

    val statsSchema = StructType(Array(
      StructField("last", new StructType(Array(
        StructField("date", TimestampType, nullable = true),
        StructField("rating", LongType, nullable = true)
      )), nullable = true),
      StructField("best", new StructType(Array(
        StructField("date", TimestampType, nullable = true),
        StructField("rating", LongType, nullable = true)
      )), nullable = true),
      StructField("record", new StructType(Array(
        StructField("win", LongType, nullable = true),
        StructField("loss", LongType, nullable = true),
        StructField("draw", LongType, nullable = true),
        StructField("time_per_move", LongType, nullable = true),
        StructField("timeout_percent", DoubleType, nullable = true)
      )), nullable = true),
      StructField("tournament", new StructType(Array(
        StructField("count", LongType, nullable = true),
        StructField("withdraw", LongType, nullable = true),
        StructField("points", LongType, nullable = true),
        StructField("highest_finish", LongType, nullable = true),
      )), nullable = true)
    ))

    val playerStatsSchema = new StructType(Array(
      StructField("chess_daily", statsSchema, nullable = true),
      StructField("chess_rapid", statsSchema, nullable = true),
      StructField("chess_blitz", statsSchema, nullable = true),
      StructField("chess_bullet", statsSchema, nullable = true),
      StructField("tactics", new StructType(Array(
        StructField("highest", LongType, nullable = true),
        StructField("lowest", LongType, nullable = true))), nullable = true),
      StructField("lessons", new StructType(Array(
        StructField("highest", LongType, nullable = true),
        StructField("lowest", LongType, nullable = true)))
        , nullable = true)
    ))

    titledPlayersJsonDf = titledPlayersJsonDf
      .withColumn("player_stats",
        from_json(getPlayerStatsInfoUdf(col("profile_player.username")), playerStatsSchema))
      .na
      .drop()
      .select(col("profile_player.*"), col("player_stats.*"))

    // -> Edges

    def getPlayerTournamentsInfo(playerName: String) : String = {
      try{
        requests.get("https://api.chess.com/pub/player/" + playerName + "/tournaments").text()
      }catch{
        case _ : requests.TimeoutException => null
        case _ : UnknownException => null
        case _ : RequestFailedException => null
        case _ : InvalidCertException => null
      }

    }

    val getPlayerTournamentsInfoUdf = udf(getPlayerTournamentsInfo(_:String) : String)

    val playerTournamentsSchema =
      new StructType(Array(
        StructField("finished", ArrayType(StringType), nullable = true)))

    var gamesDf = titledPlayersJsonDf
      .select(from_json(getPlayerTournamentsInfoUdf(col("username")), playerTournamentsSchema).alias("player_tournaments"))
      .na
      .drop()
      .select(element_at($"player_tournaments.finished", 1).alias("player_tournaments"))
      .select(split(col("player_tournaments"), "/").alias("player_tournaments"))
      .select(col("player_tournaments")(5).alias("player_tournaments"))
      .select(regexp_replace(col("player_tournaments"), "\",\"@id\":\"https:", "").alias("player_tournaments"))


    def getTournamentsInfo(tournament: String) : String = {
      try{
        if (tournament != null && tournament != ""){
          return requests.get("https://api.chess.com/pub/tournament/" + tournament).text()
        }
        null
      }catch{
        case _ : RequestFailedException =>
          null
        case _ : requests.TimeoutException =>
          null
        case _ : UnknownException =>
          null
        case _ : InvalidCertException => null
      }

    }

    val getTournamentsInfoUdf = udf(getTournamentsInfo(_:String) : String)

    val tournamentsSchema =
      new StructType(Array(
        StructField("rounds", ArrayType(StringType), nullable = true)
      ))

    gamesDf = gamesDf
      .withColumn("round_tournaments", from_json(getTournamentsInfoUdf(col("player_tournaments")), tournamentsSchema))
      .withColumn("round_tournaments", col("round_tournaments.rounds")(0))

    def getRoundTournamentsInfo(round_url: String) : String = {
      if (round_url != null){
        try{
          return requests.get(round_url).text()
        }catch {
          case _ : RequestFailedException =>
            return null
          case _ : requests.TimeoutException =>
            return null
          case _ : UnknownException =>
            return null
          case _ : InvalidCertException =>
            return null
        }
      }
      null
    }

    val getRoundTournamentsInfoUdf = udf(getRoundTournamentsInfo(_:String) : String)

    val roundTournamentsSchema =
      new StructType(Array(
        StructField("groups", ArrayType(StringType), nullable = true)
      ))

    gamesDf = gamesDf
      .na
      .drop()
      .select(col("player_tournaments"), from_json(getRoundTournamentsInfoUdf(col("round_tournaments")), roundTournamentsSchema).alias("group_round_tournaments"))
      .withColumn("group_round_tournaments", col("group_round_tournaments.groups")(0))

    def getGamesInfo(games_url: String) : String = {
      if (games_url != null){
        try{
          return requests.get(games_url).text()
        }catch {
          case _ : RequestFailedException =>
            return null
          case _ : requests.TimeoutException =>
            return null
          case _ : UnknownException =>
            return null
          case _ : InvalidCertException =>
            return null
        }
      }
      null
    }

    val getGamesInfoUdf = udf(getGamesInfo(_:String) : String)

    val roundGamesSchema =
      new StructType(Array(
        StructField("games", ArrayType(StructType(Array(
          StructField("end_time", TimestampType, nullable = true),
          StructField("time_class", StringType, nullable = true),
          StructField("white", StructType(Array(
            StructField("rating", LongType, nullable = true),
            StructField("username", StringType, nullable = true)
          )), nullable = true),
          StructField("black", StructType(Array(
            StructField("rating", LongType, nullable = true),
            StructField("username", StringType, nullable = true)
          )), nullable = true),
          StructField("eco", StringType, nullable = true)
        ))), nullable = true)
      ))

    gamesDf = gamesDf
      .na
      .drop()
      .select(col("player_tournaments"), from_json(getGamesInfoUdf(col("group_round_tournaments")), roundGamesSchema).alias("games_tournaments"))
      .select(col("player_tournaments"), array(element_at($"games_tournaments.games", 1), element_at($"games_tournaments.games", 2), element_at($"games_tournaments.games", 3)).alias("games_tournaments"))
      .select(col("player_tournaments"), explode(col("games_tournaments")).alias("games_tournaments"))
      .select(col("player_tournaments"), col("games_tournaments.*"))
      .withColumn("white", col("white.username"))
      .withColumn("black", col("black.username"))

    import org.apache.spark.sql.functions._

    titledPlayersJsonDf = titledPlayersJsonDf // para obtener códigos de países en los vértices
      .withColumn("country", substring_index(col("country"), "/", -1))
    gamesDf = gamesDf // para obtener aperturas
      .withColumn("eco", substring_index(col("eco"), "/", -1))

    // construcción del grafo

    val playersVertices = titledPlayersJsonDf
      .withColumnRenamed("username", "id").distinct()

    val gamesEdges = gamesDf
      .withColumnRenamed("white", "src")
      .withColumnRenamed("black", "dst")

    import org.graphframes.GraphFrame
    val chessGraph = GraphFrame(playersVertices, gamesEdges)
    chessGraph.cache()

    // basic queries

    println(s"Total Number of players: ${chessGraph.vertices.count()}")
    println(s"Total Number of matches in Graph: ${chessGraph.edges.count()}")

    // 1º matches donde se jugó la defensa siciliana

    chessGraph
      .edges
      .where(col("eco").contains("Sicilian-Defense"))
      .show(5)

    // 2º jugador con mayor número de seguidores de Chess.com según los matches registrados

    chessGraph
      .vertices
      .groupBy($"id")
      .agg(max("followers").alias("max_followers"))
      .select($"id", $"max_followers")
      .orderBy(desc("max_followers"))
      .show(1)

    // motif finding

    val motif = chessGraph.find("(b)-[e]->(n)")

//     3º matches donde alguno de los dos jugadores es sudafricano

    motif
      .filter("b.country == \"ZA\" OR n.country == \"ZA\"")
      .show(5)

    // 4º matches donde el jugador de blancas se registró antes del 2022-09-12 00:00

    motif
      .filter(col("b.joined") < "2022-09-10 12:35")
      .show(5)

    val inDeg = chessGraph.inDegrees
    inDeg.orderBy(desc("inDegree")).show(5, false)



  }

}
