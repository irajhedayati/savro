package ca.dataedu.savro

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class implicitsTest extends AnyFlatSpec with Matchers {

  import implicits._

  behavior.of("OptionOfEitherOfOption")

  it should "return Right('None') if the value is 'None'" in {
    None.pushDownOption() shouldBe Right(None)
  }

  it should "return Left('Left Value') if it is Option(Left('Left Value'))" in {
    Option("Left Value".asLeft[Option[Int]]).pushDownOption() shouldBe Left("Left Value")
  }

  it should "return Right('Right Value') if it is Option of right" in {
    Option(Option("Right Value").asRight[Int]).pushDownOption() shouldBe Right(Option("Right Value"))
    Option(None.asRight[Int]).pushDownOption() shouldBe Right(None)
  }
}
