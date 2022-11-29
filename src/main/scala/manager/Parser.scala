package manager

import data._
import spray.json._

import java.sql.Timestamp

// Json parser

object Parser extends DefaultJsonProtocol{
  implicit object TimestampJsonFormat extends JsonFormat[Timestamp] {
    def write(x: Timestamp): JsNumber = JsNumber(x.getTime)
    def read(value: JsValue): Timestamp = value match {
      case JsNumber(x) => new Timestamp(x.longValue)
      case x => deserializationError("Expected Timestamp as JsNumber, but got " + x)
    }
  }

  implicit def listJsonWriter[T : JsonWriter]: RootJsonWriter[List[T]] = new  RootJsonWriter[List[T]] {
    def write(list: List[T]): JsArray = JsArray(list.map(_.toJson).toVector)
  }


  implicit val jsonFormatterProfile: RootJsonFormat[Profile] = jsonFormat8(Profile.apply)
  implicit val jsonFormatterPlayerTournamentNode: RootJsonFormat[PlayerTournament] = jsonFormat(PlayerTournament, "@id")
  implicit val jsonFormatterTournament: RootJsonFormat[Tournament] = jsonFormat1(Tournament.apply)
  implicit val jsonFormatterRounds: RootJsonFormat[Round] = jsonFormat1(Round.apply)
  implicit val jsonFormatterPlayer: RootJsonFormat[Player] = jsonFormat2(Player.apply)
  implicit val jsonFormatterGameNode: RootJsonFormat[Match] = jsonFormat4(Match.apply)
  implicit val jsonFormatterGame: RootJsonFormat[MatchArchive] = jsonFormat1(MatchArchive.apply)
}
