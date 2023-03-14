/*
 * Copyright 2023 Swisscom (Schweiz) AG.
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

package com.swisscom.kafka.schemaregistry.yang;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.model.api.stmt.Import;
import org.yangcentral.yangkit.model.api.stmt.Module;
import org.yangcentral.yangkit.parser.YangParserException;
import org.yangcentral.yangkit.register.YangStatementImplRegister;
import org.yangcentral.yangkit.register.YangStatementRegister;

public class YangSchemaProvider extends AbstractSchemaProvider {

  private static final Logger log = LoggerFactory.getLogger(YangSchemaProvider.class);

  public YangSchemaProvider() {
    YangStatementImplRegister.registerImpl();
  }

  @Override
  public String schemaType() {
    return YangSchema.TYPE;
  }

  @Override
  public Optional<ParsedSchema> parseSchema(Schema schema, boolean isNew) {
    log.debug("parseSchema: {}, new: {}", schema, isNew);
    return super.parseSchema(schema, isNew);
  }

  @Override
  public Optional<ParsedSchema> parseSchema(Schema schema, boolean isNew, boolean normalize) {
    log.debug("parseSchema: {}, new: {}, normalize: {}", schema, isNew, normalize);
    return super.parseSchema(schema, isNew, normalize);
  }

  @Override
  public Optional<ParsedSchema> parseSchema(
      String schemaString, List<SchemaReference> references, boolean isNew) {
    return super.parseSchema(schemaString, references, isNew);
  }

  @Override
  public Optional<ParsedSchema> parseSchema(
      String schemaString, List<SchemaReference> references, boolean isNew, boolean normalize) {
    return super.parseSchema(schemaString, references, isNew, normalize);
  }

  @Override
  public Optional<ParsedSchema> parseSchema(String schemaString, List<SchemaReference> references) {
    return super.parseSchema(schemaString, references);
  }

  @Override
  public ParsedSchema parseSchemaOrElseThrow(Schema schema, boolean isNew, boolean normalize) {
    log.debug("parseSchemaOrElseThrow schema: {}, new: {}", schema, isNew);
    YangSchemaContext context = YangStatementRegister.getInstance().getSchemeContextInstance();
    Map<String, String> resolvedReferences = resolveReferences(schema.getReferences());

    try {
      // Parse first resolved references
      for (Map.Entry<String, String> entry : resolvedReferences.entrySet()) {
        YangSchemaUtils.parseYangString(entry.getKey(), entry.getValue(), context);
      }
      YangSchemaUtils.parseSchema(schema, context);
      context.validate();
      Module rootModule = context.getModules().get(context.getModules().size() - 1);
      for (Import imported : rootModule.getImports()) {
        // AH: do we need to resolve imports recursively?! Assuming this check was done on each one
        if (!resolvedReferences.containsKey(imported.getArgStr())) {
          throw new IllegalArgumentException("Unresolved import: " + imported);
        }
      }
      YangSchema yangSchema =
          new YangSchema(
              schema.getSchema(),
              null,
              context,
              rootModule,
              schema.getReferences(),
              resolvedReferences,
              null);
      return yangSchema;
    } catch (YangParserException e) {
      log.error("Error parsing Yang Schema", e);
      throw new IllegalArgumentException("Invalid Yang " + schema.getSchema(), e);
    } catch (Exception e) {
      log.error("Error parsing Yang Schema", e);
      throw e;
    }
  }
}
