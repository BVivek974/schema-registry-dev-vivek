/*
 * Copyright 2023 INSA Lyon.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.insa.kafka.formatter.yang.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.insa.kafka.serializers.yang.AbstractKafkaYangSchemaDeserializer;
import com.insa.kafka.serializers.yang.KafkaYangSchemaDeserializerConfig;
import com.swisscom.kafka.schemaregistry.yang.YangSchemaProvider;
import io.confluent.kafka.formatter.SchemaMessageDeserializer;
import io.confluent.kafka.formatter.SchemaMessageFormatter;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

public class YangJsonSchemaMessageFormatter extends SchemaMessageFormatter<JsonNode> {

  private static final ObjectMapper objectMapper = Jackson.newObjectMapper();

  public YangJsonSchemaMessageFormatter() {}

  @Override
  protected SchemaMessageDeserializer createDeserializer(Deserializer keyDeserializer) {
    return new YangSchemaMessageDeserializer(keyDeserializer);
  }

  @Override
  protected void writeTo(String topic, Headers headers, byte[] data, PrintStream output)
      throws IOException {
    JsonNode object = deserializer.deserialize(topic, headers, data);
    output.println(objectMapper.writeValueAsString(object));
  }

  @Override
  protected SchemaProvider getProvider() {
    return new YangSchemaProvider();
  }

  static class YangSchemaMessageDeserializer extends AbstractKafkaYangSchemaDeserializer<JsonNode>
      implements SchemaMessageDeserializer<JsonNode> {

    protected final Deserializer keyDeserializer;

    YangSchemaMessageDeserializer(Deserializer keyDeserializer) {
      this.keyDeserializer = keyDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      if (!configs.containsKey(KafkaYangSchemaDeserializerConfig.YANG_JSON_FAIL_INVALID_SCHEMA)) {
        ((Map<String, Object>) configs).put(
            KafkaYangSchemaDeserializerConfig.YANG_JSON_FAIL_INVALID_SCHEMA, "true");
        configure(deserializerConfig(configs), null);
      }
    }

    @Override
    public Deserializer getKeyDeserializer() {
      return keyDeserializer;
    }

    @Override
    public Object deserializeKey(String topic, Headers headers, byte[] payload) {
      return keyDeserializer.deserialize(topic, headers, payload);
    }

    @Override
    public JsonNode deserialize(String topic, Headers headers, byte[] payload)
        throws SerializationException {
      return (JsonNode) super.deserialize(false, topic, isKey, headers,
          payload);
    }

    @Override
    public SchemaRegistryClient getSchemaRegistryClient() {
      return schemaRegistry;
    }
  }
}
