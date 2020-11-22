package com.org.challenge.stream.utils

import java.io.InputStream

import com.org.challenge.stream.schemas.SchemaManagement
import org.apache.spark.sql.types.StructType

class Utils {
  def getResourceContentAsString(path: String): String = {
    val stream: InputStream = getClass.getResourceAsStream(path)
    scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
  }

  def getSchemaWith(schemaIdentifier: String, schemaManagement: SchemaManagement): StructType = {
    schemaManagement.getSchemaFromRegistry(schemaIdentifier)
  }
}
