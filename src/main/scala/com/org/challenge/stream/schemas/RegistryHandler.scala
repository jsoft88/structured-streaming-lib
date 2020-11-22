package com.org.challenge.stream.schemas

trait RegistryRequestType

abstract class RegistryAuthorization

abstract class RegistryRequestResponse(payload: Option[Any]) {
  val responsePayload: Option[Any] = payload
}

trait RegistryHandler {
  def getAuthorizationToken(params: Option[Seq[String]] = None): Option[RegistryAuthorization]

  def sendRequestToRegistry(
                             requestType: RegistryRequestType,
                             authorizationToken: Option[RegistryAuthorization],
                             extraParams: Option[Map[String, String]]): RegistryRequestResponse
}
