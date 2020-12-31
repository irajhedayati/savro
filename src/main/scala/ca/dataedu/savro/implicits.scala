package ca.dataedu.savro

object implicits {

  implicit class EitherOps[A](private val obj: A) extends AnyVal {

    /** Wrap a value in `Left`
      *
      * @tparam B type of `Right`
      */
    def asLeft[B]: Either[A, B] = Left(obj)

    /** Wrap a value in `Right`
      *
      * @tparam B type of `Left`
      */
    def asRight[B]: Either[B, A] = Right(obj)

  }

  implicit class Collections[A, B](listOfEither: List[Either[A, B]]) {

    /** Converts a list of Either to a an either of list. If any of them is a
      * left, takes the first one and applies a transformation and return a
      * singular left */
    final def toEitherOfList[C](leftTransformer: A => C): Either[C, List[B]] =
      listOfEither.foldRight[(List[C], List[B])](Nil, Nil) {
        case (Left(left), (lefts, rights))   => (leftTransformer(left) :: lefts, rights)
        case (Right(right), (lefts, rights)) => (lefts, right :: rights)
      } match {
        case (Nil, rights) => Right(rights)
        case (lefts, _)    => Left(lefts.head)
      }
  }

}
