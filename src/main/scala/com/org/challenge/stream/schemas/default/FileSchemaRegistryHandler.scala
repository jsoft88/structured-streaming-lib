package com.joyn.challenge.stream.schemas.default

import com.org.challenge.stream.schemas.{RegistryAuthorization, RegistryHandler, RegistryRequestResponse, RegistryRequestType}
import com.org.challenge.stream.utils.Utils

case object DefaultRequestRetrieveSchema extends RegistryRequestType {
  override def toString: String = "retrieve-schema"
}

class DefaultRegistryRequestResponse(payload: Option[Any]) extends RegistryRequestResponse(payload)

object FileSchemaRegistry {
  val ArgSchemaId = "schemaId"
}

class FileSchemaRegistryHandler extends RegistryHandler {

  override def getAuthorizationToken(params: Option[Seq[String]]): Option[RegistryAuthorization] = None

  override def sendRequestToRegistry(
                                      requestType: RegistryRequestType,
                                      authorizationToken: Option[RegistryAuthorization] = None,
                                      extraParams: Option[Map[String, String]] = None): DefaultRegistryRequestResponse = {
    val schemaIdentifier = extraParams match {
      case Some(p) => p get(FileSchemaRegistry.ArgSchemaId) match {
        case Some(id) => id
        case None => throw new Exception(s"${FileSchemaRegistry.ArgSchemaId} not present in param list")
      }
      case None => throw new Exception("Expected params for request")
    }
    new DefaultRegistryRequestResponse(payload = Some(new Utils().getResourceContentAsString(s"/schemas/${schemaIdentifier}.json")))
  }
}
