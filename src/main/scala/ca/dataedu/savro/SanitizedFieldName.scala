package ca.dataedu.savro

/**
  * @param validFieldName sanitized field name
  * @param explanation if we had to sanitize the field name, it will explain it why
  */
final case class SanitizedFieldName(validFieldName: String, explanation: Option[String])

object SanitizedFieldName {

  /**
    * Sanitize the field names to be compatible with Avro naming. The rules are:
    * {{{
    * - If there is a character not accepted in Avro (Avro fields should be
    *   "[^A-Za-z0-9_]", it replaces them with "_" character. If there are
    *   consecutive illegal characters, it uses a single "_" for all of them
    * - Drops leading "_" either from original record or from the previous step
    * - It converts all the field to lowercase
    * }}}
    */
  def apply(fieldName: String): SanitizedFieldName = {
    val sanitizedFieldName = fieldName
      .replaceAll("[^A-Za-z0-9_]+", "_")
      .replaceAll("_+$", "")
      .replaceAll("^_+", "")
    if (sanitizedFieldName.equals(fieldName)) SanitizedFieldName(fieldName, None)
    else
      SanitizedFieldName(
        sanitizedFieldName,
        Option(
          s"The original field name was '$fieldName' but some characters is not accepted in " +
          "the field name of Avro record"
        )
      )
  }
}
