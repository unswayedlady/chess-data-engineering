object Pruebas {
  def main(args: Array[String]): Unit ={

    val it = Iterator.unfold(0: Int)(s => if (s < 5) Some((s, s+1)) else None)
    while(it.hasNext) {
      println(it.next())
    }

    //    def unfold[A, S](init: S)(f: (S) => Option[(A, S)]): Iterator[A]
    //    Creates an Iterator that uses a function f to produce elements of
    //    type A and update an internal state of type S.

  }

}
