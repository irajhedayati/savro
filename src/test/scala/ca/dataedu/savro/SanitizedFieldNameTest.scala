package ca.dataedu.savro

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SanitizedFieldNameTest extends AnyFlatSpec with Matchers {

  "Field name sanitizer" should "replace illegal characters with '_'" in {
    SanitizedFieldName("address.postal").validFieldName shouldBe "address_postal"
  }

  it should "return the field without change if there is no illegal character" in {
    SanitizedFieldName("CUSTOMER_ID").validFieldName shouldBe "CUSTOMER_ID"
  }

  it should "drop the trailing '_' if there is illegal character at the end of the field name" in {
    SanitizedFieldName("List - Search Type").validFieldName shouldBe "List_Search_Type"

  }

  it should "use single '_' for consecutive illegal characters" in {
    SanitizedFieldName(" Product List Position(Custom)").validFieldName shouldBe "Product_List_Position_Custom"
  }

}
