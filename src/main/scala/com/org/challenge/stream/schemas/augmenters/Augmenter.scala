package com.org.challenge.stream.schemas.augmenters

import com.org.challenge.stream.schemas.SchemaModel

/**
 * This trait is responsible for "augmenting" the schema obtained by {@code com.org.challenge.stream.schemas.SchemaManagement}
 * and by augment we mean given the standard json representation of schemas in the library, in combination with the defined
 * types in {@code com.org.challenge.stream.schemas.types.PredefinedDomainTypes}, to represent it in another format for a
 * given serialization format, for example, an avro schema.
 *
 * @tparam B data type of the outcome of applying the {@code com.org.challenge.stream.schemas.augmenters.Augmenter#fromModelToB}
 *           method.
 */
trait Augmenter[B] {
  def fromModelToB(schemaModel: SchemaModel): B
}
