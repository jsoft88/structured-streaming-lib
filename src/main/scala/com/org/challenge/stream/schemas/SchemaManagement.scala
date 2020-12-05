package com.org.challenge.stream.schemas

import org.apache.spark.sql.types.StructType

/**
 * Trait that requires implementation by each of the {@code com.org.challenge.stream.schemas.SchemaManagement} child classes
 * The main purpose is to obtain an instance of a child class of {@code com.org.challenge.stream.schemas.SchemaModel} from
 * a given {@code org.apache.spark.sql.types.StructType}
 * @tparam T Type of the child class extending {@code com.org.challenge.stream.schemas.SchemaModel} returned by the method
 *           fromStructToModel
 */
trait ToModel[T] {
  def fromStructToModel(name: String, structType: StructType): T
}

abstract class SchemaManagement(registryHandler: RegistryHandler) {
  def getSchemaFromRegistry(schemaIdentifier: String): StructType
}
