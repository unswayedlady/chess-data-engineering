package manager

import data._
import Getters._
import manager.UnfoldIterator.Op

// Manager class intended to download data from API rest

object Manager {

  def apply(): Iterator[(Match, List[Profile])] = {

    List("GM", "WGM", "IM", "WIM", "FM", "WFM", "CM", "WCM")
      .iterator
      .flatMap(t => {
        println("Getting " + t + " titled players...")
        getTitledPlayers(t)
      })
      .flatMap(p => {
        println("Obtaining " + p + " tournaments... ")
        getPlayerTournaments(p)
      })
      .flatMap(r => {
        println("Obtaining from " + r + " round's info... ")
        getRounds(r)
      })
      .flatMap(g => {
        println("Getting from " + g + " group's info... ")
        getGroups(g)
      })
      .flatMap(getMatch)
      .unfold2(scala.collection.mutable.Set[String]())(getPlayers)
      .collect({
        case (m, Some(l)) => (m, l)
      })

    /*({
        (m, s) => {
          itCounter.next()
          if (!s.contains(m.white.username) && !s.contains(m.black.username)){
            getPlayer(m.white.username) flatMap { whiteVal =>
              getPlayer(m.black.username) map { blackVal =>
                ((m, whiteVal :: blackVal :: Nil), s + (m.white.username, m.black.username))
              }
            }
          }
          else if (!s.contains(m.white.username) && s.contains(m.black.username)){
            getPlayer(m.white.username) map{whiteVal =>
              ((m, whiteVal :: Nil), s + m.white.username)
            }

          }
          else if (s.contains(m.white.username) && !s.contains(m.black.username)){
            getPlayer(m.black.username) map{blackVal =>
              ((m, blackVal :: Nil), s + m.black.username)
            }
          }
          else{
            Some((m, Nil), s)
          }
      }})*/


  }

}

