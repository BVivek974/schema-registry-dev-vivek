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
import com.swisscom.kafka.schemaregistry.yang.YangSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.yangcentral.yangkit.common.api.validate.ValidatorResult;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.model.api.stmt.Import;
import org.yangcentral.yangkit.model.api.stmt.Module;
import org.yangcentral.yangkit.model.impl.schema.YangSchemaContextImpl;
import org.yangcentral.yangkit.parser.YangParserException;
import org.yangcentral.yangkit.register.YangStatementRegister;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaYangJsonSchemaSerializer<T> extends AbstractKafkaYangJsonSchemaSerializer<T>
    implements Serializer<T> {

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
  public byte[] serialize(String topic, T record) {
    return serialize(topic, null, record);
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T record) {
    // TODO: record use a class with the JsonNode and the Schemas. Maybe YangDataDocument?
    System.out.println("TODO_ALEX: AM I serializing?");
    if (record == null) {
      return null;
    }
    try {
      List<YangSchema> schemas = getSchema(record);
    } catch (YangParserException e) {
      throw new RuntimeException(e);
    }
    YangSchema schema = schemaCache.computeIfAbsent(record.getClass(), k -> getSchema(record));
    System.out.println("Schema is: " + schema);
    if (schema == null) {
      return null;
    }
    // TODO: serializeImpl: use JsonNode
    return serializeImpl(
        getSubjectName(topic, isKey, record, schema), topic, headers, record, schema);
  }

  // TODO: use YangDocumentData instead?
  private List<YangSchema> getSchema(T record) throws YangParserException {
    if (record instanceof YangSchemaAndValue) {
      List<String> schemasString = ((YangSchemaAndValue) record).getSchemas();
      Map<String, String> references = new HashMap<String, String>();

      List<YangSchema> schemas = new ArrayList<>();
      YangSchemaContext context = new YangSchemaContextImpl();

      for (String schemaString : schemasString) {
        String yangModuleName = "name";
        YangSchemaUtils.parseYangString(yangModuleName, schemaString, context);
        ValidatorResult validatorResult = context.validate();
        if (validatorResult.isOk()) {
          throw new IllegalArgumentException("YANG module not valid: \n" + validatorResult.getRecords().stream()
              .map(Object::toString)
              .collect(Collectors.joining("\n")));
        }
        Module lastModule = context.getModules().get(context.getModules().size() - 1);
        for (Import imported : lastModule.getImports()) {
          if (!references.containsKey(imported.getArgStr())) {
            throw new IllegalArgumentException("Unresolved import: " + imported);
          }
        }
        YangSchema yangSchema = new YangSchema(
            schemaString,
            context,
            lastModule,
            new ArrayList<>(),
            references
        );
        references.put(yangModuleName, schemaString);
        schemas.add(yangSchema);
      }

      return schemas;
    }
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
