package data

import java.sql.Timestamp

case class Profile(player_id: Long, // the non-changing Chess.com ID of this player
                   username: String, // the username of this player
                   title: Option[String], // (optional) abbreviation of chess title, if any
                   status: String, // account status: closed, closed:fair_play_violations, basic, premium, mod, staff)
                   country: String, // API location of this player's country's profile
                   followers: Int, // the number of players tracking this player's activity
                   is_streamer: Boolean, //if the member is a Chess.com streamer
                   joined: Timestamp // timestamp of registration on Chess.com
                  )
{
  override def equals(obj: Any): Boolean = {
    obj match{
      case Profile => this.player_id.equals(obj.asInstanceOf[Profile].player_id)
      case _ => false
    }
  }
}
