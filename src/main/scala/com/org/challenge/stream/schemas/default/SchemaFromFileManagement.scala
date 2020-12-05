package com.org.challenge.stream.schemas.default

import com.org.challenge.stream.schemas.types.SQLTypeMapper
import com.org.challenge.stream.schemas.{RegistryHandler, SchemaManagement, ToModel}
import io.circe._
import io.circe.parser._
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

class SchemaFromFileManagement(registryHandler: RegistryHandler) extends SchemaManagement(registryHandler)
  with ToModel[FileBasedSchemaModel] {

  override def getSchemaFromRegistry(schemaIdentifier: String): StructType = {
    try {
      val registryResponse = this.registryHandler.sendRequestToRegistry(
        DefaultRequestRetrieveSchema,
        None,
        Some(Map(FileSchemaRegistry.ArgSchemaId -> schemaIdentifier))
      )
      val schemaAsJson = registryResponse.responsePayload match {
        case None => throw new Exception("Schema registry responded None")
        case Some(res) => {
          parse(res.asInstanceOf[String]) match {
            case Left(e) => throw new Exception(e.message)
            case Right(jObject) => jObject
          }
        }
      }
      schemaAsJson.as[FileBasedSchemaModel] match {
        case Right(schema) => {
          StructType(schema
            .fields
            .foldRight(Seq[StructField]())((fl, f) => f ++ Seq(StructField(fl.name, SchemaFieldModel.map[DataType, String](new SQLTypeMapper(){}, fl.`type`), true))))
        }
        case Left(err) => throw err
      }
    } catch {
      case e: Exception => throw e
    }
  }

  override def fromStructToModel(name: String, structType: StructType): FileBasedSchemaModel = {
    FileBasedSchemaModel(
      name,
      structType.fields.map(f => SchemaFieldModel(f.name, SchemaFieldModel.reverseMap(new SQLTypeMapper(){}, f.dataType))))
  }
}
