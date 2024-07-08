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
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.yangcentral.yangkit.data.api.model.YangDataDocument;
import org.yangcentral.yangkit.model.api.stmt.Import;
import org.yangcentral.yangkit.model.api.stmt.Module;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaYangJsonSchemaSerializer<T>
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
    List<Module> modules = record.getSchemaContext().getModules();
    if (modules.isEmpty()) {
      return null;
    }
    List<List<Module>> dependenciesOrder = record.getSchemaContext().resolvesImportOrder();
    List<ParsedSchema> schemas = new ArrayList<>();
    for (List<Module> dependencies : dependenciesOrder) {
      Module tempLastModule = dependencies.get(dependencies.size() - 1);
      for (Module m : dependencies) {
        List<SchemaReference> references = getReferencesFromImport(m);
        ParsedSchema newSchema = this.schemaRegistry.parseSchema(YangSchema.TYPE,
                m.getOriginalString(), references).get();
        if (dependenciesOrder.size() != 1 || tempLastModule != m) {
          try {
            this.schemaRegistry.register(m.getArgStr(), newSchema);
          } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
          }
        }
        if (tempLastModule == m) {
          schemas.add(newSchema);
        }
      }
    }
    if (schemas.size() == 1) {
      return (YangSchema) schemas.get(0);
    }
    List<SchemaReference> moduleReferences = new ArrayList<>();
    StringBuilder schemaName = new StringBuilder();
    for (ParsedSchema m : schemas) {
      moduleReferences.add(new SchemaReference(m.name(),m.name(), -1));
      schemaName.append(m.name()).append("-");
    }
    return (YangSchema) this.schemaRegistry.parseSchema(YangSchema.TYPE,
            createYangSchema(schemaName.toString()), moduleReferences).get();
  }

  private List<SchemaReference> getReferencesFromImport(Module m) {
    List<Import> imports = m.getImports();
    List<SchemaReference> references = new ArrayList<>();
    for (Import currentImport : imports) {
      String subject = currentImport.getArgStr();
      SchemaReference ref = new SchemaReference(subject, subject, -1);
      references.add(ref);
    }
    return references;
  }

  private String createYangSchema(String name) {
    return "module " + name + "{yang-version 1.1; namespace \"zzz\"; prefix zzz;}";
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
