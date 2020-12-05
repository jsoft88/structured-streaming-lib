package com.org.challenge.stream.schemas.default

import com.org.challenge.stream.factory.SchemaManagementFactory
import org.scalatest.funsuite.AnyFunSuite

class SchemaFromFileManagementTest extends AnyFunSuite {
  test("a schema representation is returned when requested schema for a given id") {
    val schemaUsers = new SchemaFromFileManagement(new FileSchemaRegistryHandler()).getSchemaFromRegistry("users")
    assert (schemaUsers.fields.length == 4)

    val schemaPageviews = new SchemaFromFileManagement(new FileSchemaRegistryHandler()).getSchemaFromRegistry("pageviews")
    assert (schemaPageviews.fields.length == 3)
  }

  test("an exception is thrown when an id is not valid") {
    assertThrows[Exception]{
      val schemaUsers = new SchemaFromFileManagement(new FileSchemaRegistryHandler()).getSchemaFromRegistry("dummy")
    }
  }

  test("an exception is thrown when schema in registry is malformed") {
    assertThrows[Exception]{
      val schemaUsers = new SchemaFromFileManagement(new FileSchemaRegistryHandler()).getSchemaFromRegistry("failed_schema")
    }
  }

  test("from StructType a correct file based model is generated") {
    val schemaUsers = new SchemaFromFileManagement(new FileSchemaRegistryHandler()).getSchemaFromRegistry("users")
    val fm = SchemaManagementFactory
      .getSchemaManagementInstance(SchemaManagementFactory.SchemaFromFileManagement, Some(new FileSchemaRegistryHandler()))
      .asInstanceOf[SchemaFromFileManagement]
      .fromStructToModel("users", schemaUsers)

    assert(fm.topic.equals("users"))
    fm.fields.foreach(f => assert(schemaUsers.fields.filter(_.name.equals(f.name)).headOption != None))
  }
}
