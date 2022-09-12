//import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, concat_ws, element_at, explode, from_json, regexp_replace, split, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType}
import requests.RequestFailedException


object Chess {


  def main(args: Array[String]): Unit = {

//    Logger.getRootLogger.setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName("Capítulo10")
      .master("local[*]")
      .config("spark.network.timeout", "3000s")
      .config("spark.executor.heartbeatInterval", "1500s")
//      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // sacamos json con nombres de jugadores grandes maestros

    /*val responseJugadoresGM = requests.get("https://api.chess.com/pub/titled/GM")

    import spark.implicits._

    // sacamos un dataframe del json

    var jugadoresGMDf = spark.read.json(Seq(responseJugadoresGM.text()).toDS)
//    jugadoresGMDf.show() // columna players

    // lo colocamos en el formato indicado

    jugadoresGMDf = jugadoresGMDf.selectExpr("explode(players) as player")
//    jugadoresGMDf.show() // columna player

    // modificamos el dataframe anterior con la info de los jugadores

    spark.udf.register("getProfilePlayerInfo", getProfilePlayerInfo(_:String):String)

    def getProfilePlayerInfo(playerName: String) : String = {
      requests.get("https://api.chess.com/pub/player/" + playerName).text()
    }
    val getProfilePlayerInfoUdf = udf(getProfilePlayerInfo(_:String) : String)

    val profilePlayerSchema = new StructType(Array(
      new StructField("avatar", StringType, true),
      new StructField("player_id", LongType, true),
      new StructField("@id", StringType, true),
      new StructField("url", StringType, true),

      new StructField("name", StringType, true),
      new StructField("username", StringType, true),
      new StructField("title", StringType, true),
      new StructField("followers", LongType, true),

      new StructField("country", StringType, true),
      new StructField("last_online", TimestampType, true),
      new StructField("joined", TimestampType, true),
      new StructField("status", StringType, true),
      new StructField("is_streamer", BooleanType, true),
      new StructField("verified", BooleanType, true),
    ))

    jugadoresGMDf = jugadoresGMDf.select(from_json(getProfilePlayerInfoUdf(col("player")), profilePlayerSchema).alias("info-player"))
      .select(col("info-player.*"))

    jugadoresGMDf.show()*/

//    jugadoresGMDf.printSchema()
//    jugadoresGMDf.select(col("username")).show()

//    jugadoresGMDf.limit(10).write.saveAsTable("jugadoresGM")

//    spark.catalog.listTables().show()

//    spark.sql("""DROP TABLE jugadoresGM""")

    //--------------------------------------------------


    // manera de crear tablas:
//    val df = spark.read.json("data/flight-data/json/2015-summary.json") // leer json

//    df.createOrReplaceTempView("flights_temporal") // crea tabla temporal
//    // 'flights_temporal' -> existe mientras existe la SparkSession

//    df.write.saveAsTable("flights") // crear tabla permanente -> a menos
    // que se borre con el drop, se queda almacenada en el spark-warehouse

//    spark.sql("""SHOW TABLES IN default""").show() // muestra tablas del catálogo
//
//    spark.sql("""SELECT * FROM flights""").show() // muestra filas de la tabla flights
//
//    spark.catalog.listTables().show() // muestra tablas del catálogo

//    spark.sql("""show databases""").show()


    //views => Una vista es una tabla virtual cuyo contenido está definido por una consulta.

    /*// crear vista
    spark.sql(
      """
        |CREATE VIEW IF NOT EXISTS nested_data AS
        |    SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) as country, count FROM flights
        |""".stripMargin)

    //consultas

    spark.sql(""" SELECT * FROM nested_data """).show()
    spark.sql(""" SELECT * FROM nested_data """).printSchema()

    spark.sql(""" SELECT country.DEST_COUNTRY_NAME, count FROM nested_data """).show()
    spark.sql(""" SELECT country.*, count FROM nested_data """).show()

    //agregación => el groupBy necesita una función de agregación

    spark.sql(
      """
        |SELECT DEST_COUNTRY_NAME as new_name, collect_list(count) as flight_counts,
        |  collect_set(ORIGIN_COUNTRY_NAME) as origin_set
        |FROM flights GROUP BY DEST_COUNTRY_NAME
        |""".stripMargin).show()

    spark.sql(
      """
        |SELECT DEST_COUNTRY_NAME, ARRAY(1, 2, 3) FROM flights
        |""".stripMargin).show()


    spark.sql(
      """
        |SELECT DEST_COUNTRY_NAME as new_name, collect_list(count)[0]
        |FROM flights GROUP BY DEST_COUNTRY_NAME
        |""".stripMargin).show()

    spark.sql(
      """
        |  CREATE OR REPLACE TEMP VIEW flights_agg AS
        |  SELECT DEST_COUNTRY_NAME, collect_list(count) as collected_counts
        |  FROM flights GROUP BY DEST_COUNTRY_NAME
        |""".stripMargin)

    spark.sql(
      """
        |SELECT * from flights_agg
        |""".stripMargin).show()


    spark.sql(
      """
        |SELECT explode(collected_counts), DEST_COUNTRY_NAME FROM flights_agg
        |""".stripMargin).show()

    spark.sql("""SHOW FUNCTIONS""").show()

    spark.sql("""SHOW SYSTEM FUNCTIONS""").show()

    spark.sql("""SHOW USER FUNCTIONS""").show()

    spark.sql("""SHOW FUNCTIONS "s*"""").show()

    //borrar tablas
    spark.sql("""DROP TABLE flights""") // borrar tabla permanente
    spark.sql("""DROP VIEW IF EXISTS nested_data;""")*/

    //graphs - bikes

    /*val bikeStations = spark.read.option("header","true")
      .csv("data/bike-data/201508_station_data.csv")
    val tripData = spark.read.option("header","true")
      .csv("data/bike-data/201508_trip_data.csv")*/

//    bikeStations.show()
//    tripData.show()

    /*val stationVertices = bikeStations.withColumnRenamed("name", "id").distinct()
    val tripEdges = tripData
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")

    import org.graphframes.GraphFrame
    val stationGraph = GraphFrame(stationVertices, tripEdges)
    stationGraph.cache()

    println(s"Total Number of Stations: ${stationGraph.vertices.count()}")
    println(s"Total Number of Trips in Graph: ${stationGraph.edges.count()}")
    println(s"Total Number of Trips in Original Data: ${tripData.count()}")*/

//    stationGraph.edges.printSchema()

    /*import org.apache.spark.sql.functions.desc
    stationGraph.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(10)

    // in Scala
    stationGraph.edges
      .where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
      .groupBy("src", "dst").count()
      .orderBy(desc("count"))
      .show(10)*/


    // Código de grafos nuevo

    var titledPlayersJson = Seq[String]()
    val titles = Seq("GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM")

    titles.foreach(title => titledPlayersJson = requests.get("https://api.chess.com/pub/titled/" + title).text() +: titledPlayersJson)

    import spark.implicits._

    var titledPlayersJsonDf = spark.read.json(titledPlayersJson.toDS()).selectExpr("explode(players) as players").limit(50)

    // -> Vertex

    // Profile player

    def getProfilePlayerInfo(playerName: String) : String = {
      requests.get("https://api.chess.com/pub/player/" + playerName).text()
    }
    val getProfilePlayerInfoUdf = udf(getProfilePlayerInfo(_:String) : String)

    val profilePlayerSchema = new StructType(Array(
      new StructField("@id", StringType, true),
      new StructField("url", StringType, true),
      new StructField("username", StringType, true),
      new StructField("player_id", LongType, true),
      new StructField("title", StringType, true),
      new StructField("status", StringType, true),
      new StructField("name", StringType, true),
      new StructField("avatar", StringType, true),
      new StructField("location", StringType, true),
      new StructField("country", StringType, true),
      new StructField("joined", TimestampType, true),
      new StructField("last_online", TimestampType, true),
      new StructField("followers", LongType, true),
      new StructField("is_streamer", BooleanType, true),
      new StructField("verified", BooleanType, true),
      new StructField("twitch_url", BooleanType, true),
      new StructField("fide", LongType, true),
    ))

    titledPlayersJsonDf = titledPlayersJsonDf
      .select(from_json(getProfilePlayerInfoUdf(col("players")), profilePlayerSchema)
        .alias("profile_player"))


    // Player stats

    def getPlayerStatsInfo(playerName: String) : String = {
      requests.get("https://api.chess.com/pub/player/" + playerName + "/stats").text()
    }

    val getPlayerStatsInfoUdf = udf(getProfilePlayerInfo(_:String) : String)

    val statsSchema = StructType(Array(
      new StructField("last", new StructType(Array(
        new StructField("date", TimestampType, true),
        new StructField("rating", LongType, true)
      )), true),
      new StructField("best", new StructType(Array(
        new StructField("date", TimestampType, true),
        new StructField("rating", LongType, true)
      )), true),
      new StructField("record", new StructType(Array(
        new StructField("win", LongType, true),
        new StructField("loss", LongType, true),
        new StructField("draw", LongType, true),
        new StructField("time_per_move", LongType, true),
        new StructField("timeout_percent", DoubleType, true),
      )), true),
      new StructField("tournament", new StructType(Array(
        new StructField("count", LongType, true),
        new StructField("withdraw", LongType, true),
        new StructField("points", LongType, true),
        new StructField("highest_finish", LongType, true),
      )), true)
    ))

    val playerStatsSchema = new StructType(Array(
      new StructField("chess_daily", statsSchema, true),
      new StructField("chess_rapid", statsSchema, true),
      new StructField("chess_blitz", statsSchema, true),
      new StructField("chess_bullet", statsSchema, true),
      new StructField("tactics", new StructType(Array(
        new StructField("highest", LongType, true),
        new StructField("lowest", LongType, true))), true),
      new StructField("lessons", new StructType(Array(
        new StructField("highest", LongType, true),
        new StructField("lowest", LongType, true))), true)
    ))


    titledPlayersJsonDf = titledPlayersJsonDf
      .withColumn("player_stats",
        from_json(getPlayerStatsInfoUdf(col("profile_player.username")), statsSchema))
      .select(col("profile_player.*"), col("player_stats.*"))

    // -> Edges

    def getPlayerTournamentsInfo(playerName: String) : String = {
      requests.get("https://api.chess.com/pub/player/" + playerName + "/tournaments").text()
    }

    val getPlayerTournamentsInfoUdf = udf(getPlayerTournamentsInfo(_:String) : String)

    val playerTournamentsSchema =
      new StructType(Array(
        StructField("finished", ArrayType(StringType), nullable = true)))

    var gamesDf = titledPlayersJsonDf
      .select(from_json(getPlayerTournamentsInfoUdf(col("username")), playerTournamentsSchema).alias("player_tournaments"))
      .select(col("player_tournaments.finished")(0).alias("player_tournaments"))
      .select(split(col("player_tournaments"), "/").alias("player_tournaments"))
      .select(col("player_tournaments")(5).alias("player_tournaments"))
      .select(regexp_replace(col("player_tournaments"), "\",\"@id\":\"https:", "").alias("player_tournaments"))


    def getTournamentsInfo(tournament: String) : String = {
      if (tournament != null && tournament != ""){
        return requests.get("https://api.chess.com/pub/tournament/" + tournament).text()
      }
      null
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
        return requests.get(round_url).text()
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
        return requests.get(games_url).text()
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
          )), nullable = true)
        ))), nullable = true)
      ))

    gamesDf
      .na
      .drop()
      .select(col("player_tournaments"), from_json(getGamesInfoUdf(col("group_round_tournaments")), roundGamesSchema).alias("games_tournaments"))
      .select(col("player_tournaments"), explode(col("games_tournaments.games")).alias("games_tournaments"))
      .select(col("player_tournaments"), col("games_tournaments.*"))
      .withColumn("white", col("white.username"))
      .withColumn("black", col("black.username"))
      .show(5)



  }

}
