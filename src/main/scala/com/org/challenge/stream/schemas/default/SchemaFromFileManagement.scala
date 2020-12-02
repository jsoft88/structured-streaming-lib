package com.org.challenge.stream.schemas.default

import com.org.challenge.stream.schemas.{RegistryHandler, SchemaManagement}
import io.circe._
import io.circe.parser._
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.sql.types.{StructField, StructType}

class SchemaFromFileManagement(registryHandler: RegistryHandler) extends SchemaManagement(registryHandler) {

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
        case Right(schema) => StructType(schema.fields.foldRight(Seq[StructField]())((fl, f) => f ++ Seq(StructField(fl.name, fl.getSQLType(), true))))
        case Left(err) => throw err
      }
    } catch {
      case e: Exception => throw e
    }
  }
}
