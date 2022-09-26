import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

import scala.util.Try
import spray.json._
import org.graphframes.GraphFrame

import java.sql.Timestamp

case class TitledPlayers(players: List[String])

case class Profile(player_id: Long, // the non-changing Chess.com ID of this player
              username: String, // the username of this player
              title: Option[String], // (optional) abbreviation of chess title, if any
              status: String, // account status: closed, closed:fair_play_violations, basic, premium, mod, staff)
              country: String, // API location of this player's country's profile
              followers: Int, // the number of players tracking this player's activity
              is_streamer: Boolean, //if the member is a Chess.com streamer
              joined: Timestamp// timestamp of registration on Chess.com
              ) {
  override def equals(obj: Any): Boolean = {
    obj match{
      case Profile => this.player_id.equals(obj.asInstanceOf[Profile].player_id)
      case _ => false
    }

  }
}

case class PlayerTournamentNode(id: String) // id of tournament
case class PlayerTournament(finished: List[PlayerTournamentNode]) // list of tournaments
// played by user

case class Tournament(rounds: List[String]) // link of rounds of tournaments

case class Round(groups: List[String]) // link of groups of rounds of tournaments

case class Player(username: String) // player username
case class GameNode(white: Player, //white player
                    black: Player, //black player
                    eco:Option[String] // opening
                   )
case class Game(games: List[GameNode], players: List[Player])

object myJsonProtocol extends DefaultJsonProtocol{

  implicit object TimestampJsonFormat extends JsonFormat[Timestamp] {
    def write(x: Timestamp): JsNumber = JsNumber(x.getTime)
    def read(value: JsValue): Timestamp = value match {
      case JsNumber(x) => new Timestamp(x.longValue * 1000)
      case x => deserializationError("Expected Timestamp as JsNumber, but got " + x)
    }
  }

  implicit val jsonFormatterTitledPlayers: RootJsonFormat[TitledPlayers] = jsonFormat1(TitledPlayers.apply)
  implicit val jsonFormatterProfile: RootJsonFormat[Profile] = jsonFormat8(Profile.apply)
  implicit val jsonFormatterPlayerTournamentNode: RootJsonFormat[PlayerTournamentNode] = jsonFormat(PlayerTournamentNode, "@id")
  implicit val jsonFormatterPlayerTournament: RootJsonFormat[PlayerTournament] = jsonFormat1(PlayerTournament.apply)
  implicit val jsonFormatterTournament: RootJsonFormat[Tournament] = jsonFormat1(Tournament.apply)
  implicit val jsonFormatterRounds: RootJsonFormat[Round] = jsonFormat1(Round.apply)
  implicit val jsonFormatterPlayer: RootJsonFormat[Player] = jsonFormat1(Player.apply)
  implicit val jsonFormatterGameNode: RootJsonFormat[GameNode] = jsonFormat3(GameNode.apply)
  implicit val jsonFormatterGame: RootJsonFormat[Game] = jsonFormat2(Game.apply)
}

object Chess {

  def main(args: Array[String]): Unit ={

    import myJsonProtocol._

    val spark = SparkSession
      .builder
      .appName("Chess")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1.- Get titled players

    val titles = List("GM", "WGM", "IM", "WIM", "FM", "WFM", "CM", "WCM")

    val titled_players : List[String] =
     titles.flatMap { title =>
       Try(requests.get("https://api.chess.com/pub/titled/" + title).text.parseJson.convertTo[TitledPlayers].players)
         .fold(_ => List(), identity)
         .take(1)
     }

    println(titled_players)
    println(titled_players.size)

    // 2.- Get profiles

    var profiles : List[Profile] =
      titled_players.flatMap { name =>
        Try(requests.get("https://api.chess.com/pub/player/" + name).text.parseJson.convertTo[Profile])
          .fold(_ => List(), profile => List(profile))
      }

    println(profiles)
    println(profiles.size)

    // 3.- Get tournaments

    val tournaments : List[String] = titled_players.flatMap {name =>
      Try(requests.get("https://api.chess.com/pub/player/" + name + "/tournaments").text.parseJson.convertTo[PlayerTournament].finished)
        .fold(_ => List(), l => l.take(2))
        .map(_.id)
    }

    println(tournaments)
    println(tournaments.size)

    // 4.- Get rounds

    val rounds : List[String] = tournaments flatMap { url =>
      Try(requests.get(url).text.parseJson.convertTo[Tournament].rounds)
        .fold(_ => List(), l => List(l.head))
    }

    println(rounds)
    println(rounds.size)

    // 5.- Get groups

    val groups : List[String] = rounds flatMap { url =>
      Try(requests.get(url).text.parseJson.convertTo[Round].groups)
        .fold(_ => List(), l => List(l.head))
    }

    println(groups)
    println(groups.size)

    // 6.- Get matches

    val matches : List[(String, String, Option[String])] = groups flatMap {url =>
      Try(requests.get(url).text.parseJson.convertTo[Game].games)
      .fold(_ => List(), l=>l.take(2))
      .map(g => (g.white.username, g.black.username, g.eco))
    }

    println(matches)
    println(matches.size)

    // 7.- Fill profiles list with players from matches info

    profiles = matches.map(m=>
      (Try(requests.get("https://api.chess.com/pub/player/" + m._1).text.parseJson.convertTo[Profile])
        .fold(_ => List(), List(_))
        ,
      Try(requests.get("https://api.chess.com/pub/player/" + m._2).text.parseJson.convertTo[Profile])
        .fold(_ => List(), List(_))
      ))
      .filter(!profiles.contains(_))
      .flatMap(t => t._1 ++ t._2) ++ profiles

    println(profiles)
    println(profiles.size)

    // 8.- Generate graph

    val vertices = spark.sqlContext.createDataFrame(profiles).toDF("player_id",
      "username",
      "title",
      "status",
      "country",
      "followers",
      "is_streamer",
      "joined")

    val edges = spark.sqlContext.createDataFrame(matches).toDF("white", "black", "eco")

    val playersVertices = vertices
      .withColumnRenamed("username", "id")
      .withColumn("country", substring_index(col("country"), "/", -1))

    playersVertices.show(50)

    val gamesEdges = edges
      .withColumnRenamed("white", "src")
      .withColumnRenamed("black", "dst")
      .withColumn("src", lower(col("src")))
      .withColumn("dst", lower(col("dst")))

    val chessGraph = GraphFrame(playersVertices, gamesEdges)
    chessGraph.cache()

    // Basic queries

    // 1º Matches where sicilian was played

    chessGraph
      .edges
      .where(col("eco").contains("Sicilian-Defense"))
      .show(5)

    // 2º Player that got max number of followers in Chess.com

    chessGraph
      .vertices
      .groupBy(col("id"))
      .agg(max("followers").alias("max_followers"))
      .select(col("id"), col("max_followers"))
      .orderBy(desc("max_followers"))
      .show(1)

    // Motif finding

    val motif = chessGraph.find("(b)-[e]->(n)")

    // 3º Matches where some player was american

    motif
      .filter("b.country == \"US\" OR n.country == \"US\"")
      .show()

    // 4º Matches where black's player registered before 2015-09-12 00:00

    motif
      .filter(col("b.joined") < "2015-09-12 00:00")
      .show()

    // Graph algorithms

    // 5º Strongly connected components
    // Get the max number of sub-communities of players

    spark.sparkContext.setCheckpointDir("src\\checkpoints")

    val result = chessGraph
      .stronglyConnectedComponents
      .maxIter(10)
      .run()

    result
      .groupBy("component")
      .count()
      .show()

    // 6º Breadth first search

    // Get possible paths (depth 3) from node with status staff to
    // another one that has +1000 followers

    chessGraph
      .bfs
      .fromExpr("status = 'staff'")
      .toExpr("followers > 1000")
      .maxPathLength(3)
      .run()
      .show()

    // 7º In degrees and out degrees

    // Get player with max white plays (indegree, as src is for white)
    // and black plays (outdegree, as dst is for black)

    val maxWhitePlays = chessGraph.inDegrees
    maxWhitePlays.orderBy(desc("inDegree")).show(1)

    val maxBlackPlays = chessGraph.outDegrees
    maxBlackPlays.orderBy(desc("outDegree")).show(1)

    // 8º PageRank

    //Identify important players based on played matches.

    chessGraph
      .pageRank
      .resetProbability(0.15)
      .tol(0.01)
      .run()
      .vertices
      .orderBy(desc("pagerank"))
      .show()



  }

}
