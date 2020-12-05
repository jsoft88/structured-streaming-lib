package com.org.challenge.stream.schemas.augmenters

import com.org.challenge.stream.schemas.SchemaModel
import com.org.challenge.stream.schemas.default.SchemaFieldModel
import com.org.challenge.stream.schemas.types.AvroTypeMapper
import org.apache.avro.{Schema, SchemaBuilder}

/**
 * An augmenter out of the box provided by the library is SchemaToAvro, which takes an instance of
 * @{code com.org.challenge.stream.schemas.SchemaModel} and produces an avro schema.
 * Note the inline implementation of traits based on {@code com.org.challenge.stream.schemas.types.TypeMapper} passed as
 * argument to the implementation of {@code com.org.challenge.stream.schemas.SchemaModel}.
 */
case class SchemaToAvro() extends Augmenter[Schema] {
  override def fromModelToB(schemaModel: SchemaModel): Schema = {
    var fieldsAssembler = SchemaBuilder.record(schemaModel.name).fields()
    schemaModel.fields.foreach(fm => {
      fieldsAssembler = fieldsAssembler.name(fm.name).`type`(SchemaFieldModel.map[Schema.Type, String](new AvroTypeMapper(){}, fm.`type`).getName).noDefault()
    })
    fieldsAssembler.endRecord()
  }
}
