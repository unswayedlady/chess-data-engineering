package manager

//Unfold

/** Creates an iterator that uses a function `f` to produce elements of
 * type `A` and update an internal state of type `S`.
 */

final class UnfoldIterator[A, S](init: S)(f: S => Option[(A, S)]) extends Iterator[A] {
  private[this] var state: S = init
  private[this] var nextResult: Option[(A, S)] = null

  override def hasNext: Boolean = {
    if (nextResult eq null) {
      nextResult = {
        val res = f(state)
        if (res eq null) throw new NullPointerException("null during unfold")
        res
      }
      state = null.asInstanceOf[S] // allow GC
    }
    nextResult.isDefined
  }

  override def next(): A = {
    if (hasNext) {
      val (value, newState) = nextResult.get
      state = newState
      nextResult = null
      value
    } else Iterator.empty.next()
  }
}

object UnfoldIterator{
  implicit class UnfoldOf[A](it: Iterator.type) {
    def unfold[A, S](init: S)(f: S => Option[(A, S)]): Iterator[A] =
      new UnfoldIterator(init)(f)
  }

  // Implicit class => unfold

  implicit class Op[M](it: Iterator[M]){
    def unfold2[A,S](s : S)(f: (M, S) => Option[(A, S)]): Iterator[A] = {
      Iterator.unfold[A, (S, Iterator[M])]((s, it) : (S, Iterator[M]))({
        case (s, it) =>
          if (!it.hasNext) None //: Option[(A, (S, Iterator[M]))]
          else{
            f(it.next(), s)  match{
              case Some(v) => Some(v._1, (v._2, it)) //: Option[(A, (S, Iterator[M]))]
              case _ => None
            }
          }
      })


    }
  }
}


