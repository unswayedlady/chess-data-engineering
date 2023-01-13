package data

case class Match(white: Player,
                 black: Player,
                 eco: Option[String],
                 pgn: String)