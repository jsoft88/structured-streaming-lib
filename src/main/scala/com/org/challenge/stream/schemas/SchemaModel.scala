package com.org.challenge.stream.schemas

import com.org.challenge.stream.schemas.default.SchemaFieldModel

abstract class SchemaModel(val name: String, val fields: Seq[SchemaFieldModel]) {

}
