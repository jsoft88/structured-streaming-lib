package com.org.challenge.stream.schemas.augmenters

import com.org.challenge.stream.schemas.SchemaModel
import com.org.challenge.stream.schemas.default.SchemaFieldModel
import com.org.challenge.stream.schemas.types.{AvroTypeMapper, SQLTypeMapper}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.types.StructType

/**
 * An augmenter out of the box provided by the library is SchemaToAvro, which takes an instance of
 * @{code org.apache.sql.types.StructType} and produces an avro schema.
 */
case class SchemaToAvro() extends Augmenter[Schema] {

  override def fromModelToB(name: String, schema: StructType): Schema = {
    var fieldsAssembler = SchemaBuilder.record(name).fields()
    schema.fields.foreach(fm => {
      fieldsAssembler = fieldsAssembler
        .name(fm.name)
        .`type`(new AvroTypeMapper(){}.map(new SQLTypeMapper(){}.reverseMap(fm.dataType)).getName).noDefault()
    })
    fieldsAssembler.endRecord()
  }
}
