package data

import java.sql.Timestamp

case class Profile(player_id: Long,
                   username: String,
                   title: Option[String],
                   status: String,
                   country: String,
                   followers: Int,
                   is_streamer: Boolean,
                   joined: Timestamp
                  )
{
  override def equals(obj: Any): Boolean = {
    obj match{
      case Profile => this.player_id.equals(obj.asInstanceOf[Profile].player_id)
      case _ => false
    }
  }
}