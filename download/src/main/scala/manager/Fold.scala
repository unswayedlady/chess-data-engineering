package manager

import scala.util.{Failure, Success, Try}

object Fold{
  implicit class TryFold[T](t: Try[T]){
    def fold[U](fa: Throwable => U, fb: T => U): U = {
      t match{
        case Success(v) => fb(v)
        case Failure(v) => fa(v)
      }
    }
  }
}

