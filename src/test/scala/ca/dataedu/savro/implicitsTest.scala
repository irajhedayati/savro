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

  behavior.of("StringOps")

  it should "return 'true' if string is CamelCase once isCamelCase is called" in {
    "EnrichedTrip".isCamelCase shouldBe true
    "Trip".isCamelCase shouldBe true
  }

  it should "return 'false' if string is not CamelCase once isCamelCase is called" in {
    "enriched_trip".isCamelCase shouldBe false
    "enriched-trip".isCamelCase shouldBe false
    "trip".isCamelCase shouldBe false
  }

  it should "return 'true' if string is snake-case once isSnakeCase is called" in {
    "enriched_trip".isSnakeCase shouldBe true
    "trip".isSnakeCase shouldBe true
  }

  it should "return 'false' if string is not snake-case once isSnakeCase is called" in {
    "EnrichedTrip".isSnakeCase shouldBe false
    "enriched-trip".isSnakeCase shouldBe false
  }

}
