package com.org.challenge.stream.schemas.augmenters

import com.org.challenge.stream.factory.SchemaManagementFactory
import com.org.challenge.stream.schemas.default.{FileSchemaRegistryHandler, SchemaFromFileManagement}
import org.scalatest.funsuite.AnyFunSuite

class SchemaToAvroTest extends AnyFunSuite {
  test("A correct avro schema is generated from model schema") {
    val schemaFromFileManagement = SchemaManagementFactory
      .getSchemaManagementInstance(SchemaManagementFactory.SchemaFromFileManagement, Some(new FileSchemaRegistryHandler()))
      .asInstanceOf[SchemaFromFileManagement]

    val schemaAsStruct = schemaFromFileManagement.getSchemaFromRegistry("users")

    // Augment with avro schema
    val avroAugmenter = SchemaToAvro().fromModelToB(schemaFromFileManagement.fromStructToModel("users", schemaAsStruct))
    assert(avroAugmenter.getName.equals("users"))
    schemaAsStruct.fields.foreach(f => {
      assert(avroAugmenter.getField(f.name) != null)
    })
  }
}
