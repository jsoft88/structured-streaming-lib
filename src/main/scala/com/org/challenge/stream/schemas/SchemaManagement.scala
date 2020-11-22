package com.org.challenge.stream.schemas

import org.apache.spark.sql.types.StructType

abstract class SchemaManagement(registryHandler: RegistryHandler) {
  def getSchemaFromRegistry(schemaIdentifier: String): StructType
}
