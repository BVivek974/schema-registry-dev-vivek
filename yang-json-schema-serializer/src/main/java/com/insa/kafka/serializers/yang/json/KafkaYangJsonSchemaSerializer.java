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

package com.insa.kafka.serializers.yang.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.swisscom.kafka.schemaregistry.yang.YangSchema;
import com.swisscom.kafka.schemaregistry.yang.YangSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.yangcentral.yangkit.data.api.model.YangDataDocument;

import java.io.IOException;
import java.util.Map;

public class KafkaYangJsonSchemaSerializer
        extends AbstractKafkaYangJsonSchemaSerializer<JsonNode>
        implements Serializer<YangDataDocument> {

  private static int DEFAULT_CACHE_CAPACITY = 1000;

  private Map<Class<?>, YangSchema> schemaCache;

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaYangJsonSchemaSerializer() {
    this.schemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  }

  public KafkaYangJsonSchemaSerializer(SchemaRegistryClient client) {
    this.schemaRegistry = client;
    this.ticker = ticker(client);
    this.schemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  }

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaYangJsonSchemaSerializerConfig(config));
  }

  @Override
  public byte[] serialize(String topic, YangDataDocument record) {
    return serialize(topic, null, record);
  }

  @Override
  public byte[] serialize(String topic, Headers headers, YangDataDocument record) {
    System.out.println("TODO_ALEX: AM I serializing?");
    if (record == null) {
      return null;
    }
    YangSchema schema = schemaCache.computeIfAbsent(record.getClass(), k -> getSchema(record));
    System.out.println("Schema is: " + schema);
    if (schema == null) {
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode data = mapper.createObjectNode();
    JsonNode node;
    try {
      node = mapper.readTree(record.getDocString());
      data.set("data", node);
    } catch (JsonProcessingException e) {
      return null;
    }

    return serializeImpl(
      getSubjectName(topic, isKey, record, schema), topic, headers, data, schema);
  }

  private YangSchema getSchema(YangDataDocument record) {
    String[] modules = record.getModulesStrings();
    Schema schema = new Schema(null, null, null, new SchemaString(modules[0]));
    YangSchemaProvider yangSchemaProvider = new YangSchemaProvider();
    return (YangSchema) yangSchemaProvider.parseSchemaOrElseThrow(schema, true,true);
  }

  @Override
  public void close() {
    try {
      super.close();
    } catch (IOException e) {
      throw new RuntimeException("Exception while closing serializer", e);
    }
  }
}
