package com.org.challenge.stream.schemas.types

/**
 * The basic functionality of this trait is to allow the mapping from domain defined types
 * as in {@class com.org.challenge.stream.schemas.types.PredefinedDomainTypes} to avro/sql types and viceversa
 * TODO: consider making, in subsequent modifications, this trait and its extending traits, available for mixins in SchemaModel
 * TODO: right now we are just taking as parameters in {@com.org.challenge.stream.schemas.default.FileModel}
 * @tparam A can be an an avro/sql type or a domain defined type
 * @tparam B can be an an avro/sql type or a domain defined type
 */
trait TypeMapper[A, B] {
  /**
   * mapping from type B to type A
   * @param `type` can be an an avro/sql type or a domain defined type
   * @return can be an an avro/sql type or a domain defined type
   * @throws Exception if no mapping was found for parameters
   */
  def map(implicit `type`: B): A

  /**
   * mapping from type A to type B
   * @param `type` can be an an avro/sql type or a domain defined type
   * @return can be an an avro/sql type or a domain defined type
   * @throws Exception if no mapping was found for parameters
   */
  def reverseMap(implicit `type`: A): B
}
