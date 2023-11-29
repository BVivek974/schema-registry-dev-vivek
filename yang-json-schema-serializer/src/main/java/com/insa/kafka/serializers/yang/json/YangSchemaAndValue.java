package com.insa.kafka.serializers.yang.json;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Objects;

public class YangSchemaAndValue {

  private final List<String> schemas;
  private final JsonNode value;

  public YangSchemaAndValue(List<String> schemas, JsonNode value) {
    this.schemas = schemas;
    this.value = value;
  }

  public List<String> getSchemas() {
    return schemas;
  }

  public JsonNode getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    YangSchemaAndValue that = (YangSchemaAndValue) o;
    return Objects.equals(schemas, that.schemas) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemas, value);
  }
}
