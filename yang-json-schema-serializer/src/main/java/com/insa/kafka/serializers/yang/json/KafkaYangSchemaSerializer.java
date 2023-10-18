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

import com.swisscom.kafka.schemaregistry.yang.YangSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaYangSchemaSerializer<T> extends AbstractKafkaYangSchemaSerializer<T>
    implements Serializer<T> {

  private static int DEFAULT_CACHE_CAPACITY = 1000;

  private Map<Class<?>, YangSchema> schemaCache;

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaYangSchemaSerializer() {
    this.schemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  }

  public KafkaYangSchemaSerializer(SchemaRegistryClient client) {
    this.schemaRegistry = client;
    this.ticker = ticker(client);
    this.schemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  }

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaYangSchemaSerializerConfig(config));
  }

  @Override
  public byte[] serialize(String topic, T record) {
    return serialize(topic, null, record);
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T record) {
    System.out.println("TODO_ALEX: AM I serializing?");
    if (record == null) {
      return null;
    }
    YangSchema schema = schemaCache.computeIfAbsent(record.getClass(), k -> getSchema(record));
    System.out.println("Schema is: " + schema);
    if (schema == null) {
      return null;
    }

    return serializeImpl(
        getSubjectName(topic, isKey, record, schema), topic, headers, record, schema);
  }

  private YangSchema getSchema(T record) {
    //TODO_ALEX:
    return null;
  }

  @Override
  public void close() {
    super.close();
  }
}
