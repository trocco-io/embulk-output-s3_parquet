package org.embulk.output.s3_parquet

import com.fasterxml.jackson.core.{JsonFactory, JsonToken}
import org.msgpack.value.{Value, ValueFactory}

import scala.jdk.CollectionConverters._

// ref: http://github.com/embulk/embulk/blob/maintain-v0.9/embulk-core/src/main/java/org/embulk/spi/json/JsonParser.java
class JsonParser {
  private val factory: JsonFactory = {
    val f = new JsonFactory()
    f.enable(
      com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS
    )
    f.enable(
      com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS
    )
    f
  }

  def parse(json: String): Value = {
    val parser =
      try {
        factory.createParser(json)
      }
      catch {
        case ex: Exception =>
          throw new JsonParseException(
            s"Failed to parse JSON: ${sampleJsonString(json)}",
            ex
          )
      }

    try {
      val token = parser.nextToken()
      if (token == null) {
        throw new JsonParseException("Unable to parse empty string")
      }
      jsonTokenToValue(parser, token, json)
    }
    catch {
      case ex: com.fasterxml.jackson.core.JsonParseException =>
        throw new JsonParseException(
          s"Failed to parse JSON: ${sampleJsonString(json)}",
          ex
        )
      case ex: JsonParseException =>
        throw ex
      case ex: RuntimeException =>
        throw new JsonParseException(
          s"Failed to parse JSON: ${sampleJsonString(json)}",
          ex
        )
    }
    finally {
      parser.close()
    }
  }

  private def sampleJsonString(json: String): String = {
    if (json.length < 100) {
      json
    }
    else {
      json.substring(0, 97) + "..."
    }
  }

  private def jsonTokenToValue(
      parser: com.fasterxml.jackson.core.JsonParser,
      token: JsonToken,
      json: String
  ): Value = {
    token match {
      case JsonToken.VALUE_NULL =>
        ValueFactory.newNil()

      case JsonToken.VALUE_TRUE =>
        ValueFactory.newBoolean(true)

      case JsonToken.VALUE_FALSE =>
        ValueFactory.newBoolean(false)

      case JsonToken.VALUE_NUMBER_FLOAT =>
        ValueFactory.newFloat(parser.getDoubleValue)

      case JsonToken.VALUE_NUMBER_INT =>
        try {
          ValueFactory.newInteger(parser.getLongValue)
        }
        catch {
          case _: com.fasterxml.jackson.core.JsonParseException =>
            ValueFactory.newInteger(parser.getBigIntegerValue)
        }

      case JsonToken.VALUE_STRING =>
        ValueFactory.newString(parser.getText)

      case JsonToken.START_ARRAY =>
        val list = scala.collection.mutable.ArrayBuffer[Value]()
        var continue = true
        while (continue) {
          val nextToken = parser.nextToken()
          if (nextToken == JsonToken.END_ARRAY) {
            continue = false
          }
          else if (nextToken == null) {
            throw new JsonParseException(
              s"Unexpected end of JSON at ${parser.getTokenLocation} while expecting an element of an array: ${sampleJsonString(json)}"
            )
          }
          else {
            list += jsonTokenToValue(parser, nextToken, json)
          }
        }
        ValueFactory.newArray(list.asJava)

      case JsonToken.START_OBJECT =>
        val map = scala.collection.mutable.Map[Value, Value]()
        var continue = true
        while (continue) {
          val nextToken = parser.nextToken()
          if (nextToken == JsonToken.END_OBJECT) {
            continue = false
          }
          else if (nextToken == null) {
            throw new JsonParseException(
              s"Unexpected end of JSON at ${parser.getTokenLocation} while expecting a key of object: ${sampleJsonString(json)}"
            )
          }
          else {
            val key = parser.getCurrentName
            if (key == null) {
              throw new JsonParseException(
                s"Unexpected token $nextToken at ${parser.getTokenLocation}: ${sampleJsonString(json)}"
              )
            }
            val valueToken = parser.nextToken()
            if (valueToken == null) {
              throw new JsonParseException(
                s"Unexpected end of JSON at ${parser.getTokenLocation} while expecting a value of object: ${sampleJsonString(json)}"
              )
            }
            val value = jsonTokenToValue(parser, valueToken, json)
            map.put(ValueFactory.newString(key), value)
          }
        }
        ValueFactory.newMap(map.asJava)

      case _ =>
        throw new JsonParseException(
          s"Unexpected token $token at ${parser.getTokenLocation}: ${sampleJsonString(json)}"
        )
    }
  }
}

class JsonParseException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
