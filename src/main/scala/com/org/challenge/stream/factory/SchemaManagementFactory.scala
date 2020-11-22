package com.org.challenge.stream.factory

import com.joyn.challenge.stream.schemas.default.{FileSchemaRegistryHandler, SchemaFromFileManagement}
import com.org.challenge.stream.schemas.{RegistryHandler, SchemaManagement}

trait SchemaManagementType

object SchemaManagementFactory {
  case object SchemaFromFileManagement extends SchemaManagementType {
    override def toString: String = "file-based"
  }

  val AllManagementTypes = Seq(
    SchemaFromFileManagement.toString
  )

  def getSchemaManagementInstance(schemaManagementType: SchemaManagementType, schemaRegistryHandler: Option[RegistryHandler]): SchemaManagement = {
    schemaManagementType match {
      case SchemaManagementFactory.SchemaFromFileManagement => new SchemaFromFileManagement(schemaRegistryHandler.getOrElse(new FileSchemaRegistryHandler()))
      case _ => throw new Exception("Schema manager type not found")
    }
  }

  def getSchemaManagementInstance(schemaManagementType: String, schemaRegistryHandler: Option[RegistryHandler] = None): SchemaManagement = {
    SchemaManagementFactory.AllManagementTypes.filter(_.toString.equalsIgnoreCase(schemaManagementType)).headOption match {
      case Some(mtch) => this.getSchemaManagementInstance(mtch, schemaRegistryHandler)
      case None => throw new Exception("Schema manager type not found")
    }
  }
}
