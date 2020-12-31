package ca.dataedu.savro

/**
  * @param validFieldName sanitized field name
  * @param explanation if we had to sanitize the field name, it will explain it why
  */
final case class SanitizedFieldName(validFieldName: String, explanation: Option[String])

object SanitizedFieldName {

  val InvalidChars = "[-+]?[0-9]*\\.[0-9]+"

  /** Replace invalid characters from the field name with `_` */
  def apply(fieldName: String): SanitizedFieldName = {
    val sanitizedFieldName = fieldName.replaceAll(InvalidChars, "_")
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
