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

package io.confluent.kafka.schemaregistry.rest.yang;

import com.swisscom.kafka.schemaregistry.yang.YangSchema;
import com.swisscom.kafka.schemaregistry.yang.YangSchemaUtils;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import org.junit.Assert;
import org.junit.Test;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.model.api.stmt.Module;
import org.yangcentral.yangkit.register.YangStatementRegister;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RestApiTest extends ClusterTestHarness {

  private static final Random random = new Random();

  public RestApiTest() {
    super(1, true);
  }

  public static List<String> getRandomYangSchemas(int num) {
    List<String> schemas = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      int index = random.nextInt(Integer.MAX_VALUE);
      String schema =
          "module a"
              + index
              + " {\n"
              + "  yang-version \"1.1\";\n"
              + "  namespace \"urn:a\";\n"
              + "  prefix \"a"
              + index
              + "\";\n"
              + "  revision \"2023-02-06\";\n"
              + "  typedef aType {\n"
              + "    type \"int8\";\n"
              + "  }\n"
              + "  container x {\n"
              + "    leaf aLeaf {\n"
              + "      type \"aType\";\n"
              + "      description\n"
              + "        \"Example leaf\";\n"
              + "    }\n"
              + "  }\n"
              + "}";
      schemas.add(schema);
    }
    return schemas;
  }

  public static void registerAndVerifySchema(
      RestService restService, String schemaString, int expectedId, String subject)
      throws IOException, RestClientException {
    registerAndVerifySchema(
        restService, schemaString, Collections.emptyList(), expectedId, subject);
  }

  public static void registerAndVerifySchema(
      RestService restService,
      String schemaString,
      List<SchemaReference> references,
      int expectedId,
      String subject)
      throws IOException, RestClientException {
    int registeredId =
        restService.registerSchema(schemaString, YangSchema.TYPE, references, subject);
    Assert.assertEquals("Registering a new schema should succeed", expectedId, registeredId);
    Assert.assertEquals(
        "Registered schema should be found",
        schemaString.trim(),
        restService.getId(expectedId).getSchemaString().trim());
  }

  @Test
  public void testBasic() throws Exception {
    String subject1 = "testTopic1";
    String subject2 = "testTopic2";
    int schemasInSubject1 = 10;
    List<Integer> allVersionsInSubject1 = new ArrayList<>();
    List<String> allSchemasInSubject1 = getRandomYangSchemas(schemasInSubject1);
    int schemasInSubject2 = 5;
    List<Integer> allVersionsInSubject2 = new ArrayList<>();
    List<String> allSchemasInSubject2 = getRandomYangSchemas(schemasInSubject2);
    List<String> allSubjects = new ArrayList<>();

    // test getAllSubjects with no existing data
    assertEquals(
        "Getting all subjects should return empty",
        allSubjects,
        restApp.restClient.getAllSubjects());

    // test registering and verifying new schemas in subject1
    int schemaIdCounter = 1;
    for (int i = 0; i < schemasInSubject1; i++) {
      String schema = allSchemasInSubject1.get(i);
      int expectedVersion = i + 1;
      registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter, subject1);
      schemaIdCounter++;
      allVersionsInSubject1.add(expectedVersion);
    }
    allSubjects.add(subject1);

    // test re-registering existing schemas
    for (int i = 0; i < schemasInSubject1; i++) {
      int expectedId = i + 1;
      String schemaString = allSchemasInSubject1.get(i);
      int foundId =
          restApp.restClient.registerSchema(
              schemaString, YangSchema.TYPE, Collections.emptyList(), subject1);
      assertEquals(
          "Re-registering an existing schema should return the existing version",
          expectedId,
          foundId);
    }

    // test registering schemas in subject2
    for (int i = 0; i < schemasInSubject2; i++) {
      String schema = allSchemasInSubject2.get(i);
      int expectedVersion = i + 1;
      registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter, subject2);
      schemaIdCounter++;
      allVersionsInSubject2.add(expectedVersion);
    }
    allSubjects.add(subject2);

    // test getAllVersions with existing data
    assertEquals(
        "Getting all versions from subject1 should match all registered versions",
        allVersionsInSubject1,
        restApp.restClient.getAllVersions(subject1));
    assertEquals(
        "Getting all versions from subject2 should match all registered versions",
        allVersionsInSubject2,
        restApp.restClient.getAllVersions(subject2));

    // test getAllSubjects with existing data
    assertEquals(
        "Getting all subjects should match all registered subjects",
        allSubjects,
        restApp.restClient.getAllSubjects());
  }

  @Test
  public void testSchemaReferences() throws Exception {
    Map<String, String> schemas = getYangSchemaWithDependencies();
    String subject = "ref";
    registerAndVerifySchema(restApp.restClient, schemas.get("ref"), 1, subject);

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get("root"));
    request.setSchemaType(YangSchema.TYPE);
    SchemaReference ref = new SchemaReference("ref", "ref", 1);
    List<SchemaReference> refs = Collections.singletonList(ref);
    request.setReferences(refs);
    int registeredId = restApp.restClient.registerSchema(request, "root", false);
    assertEquals("Registering a new schema should succeed", 2, registeredId);

    SchemaString schemaString = restApp.restClient.getId(2);
    // the newly registered schema should be immediately readable on the leader
    assertEquals(
        "Registered schema should be found", schemas.get("root"), schemaString.getSchemaString());

    assertEquals("Schema dependencies should be found", refs, schemaString.getReferences());

    YangSchemaContext context = YangStatementRegister.getInstance().getSchemeContextInstance();
    YangSchemaUtils.parseYangString("root", RootYangSchema, context);
    Module module = context.getModules().get(0);

    YangSchema schema = new YangSchema(null, context, module, refs, Collections.emptyMap(), null);
    Schema registeredSchema =
        restApp.restClient.lookUpSubjectVersion(
            schema.canonicalString(), YangSchema.TYPE, schema.references(), "root", false);
    assertEquals("Registered schema should be found", 2, registeredSchema.getId().intValue());
  }

  @Test(expected = RestClientException.class)
  public void testSchemaMissingReferences() throws Exception {
    Map<String, String> schemas = getYangSchemaWithDependencies();

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get("root"));
    request.setSchemaType(YangSchema.TYPE);
    request.setReferences(Collections.emptyList());
    restApp.restClient.registerSchema(request, "root", false);
  }

  @Test
  public void testBad() throws Exception {
    String subject1 = "testTopic1";
    List<String> allSubjects = new ArrayList<>();

    // test getAllSubjects with no existing data
    assertEquals(
        "Getting all subjects should return empty",
        allSubjects,
        restApp.restClient.getAllSubjects());

    try {
      registerAndVerifySchema(restApp.restClient, getBadSchema(), 1, subject1);
      fail("Registering bad schema should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals("Invalid schema", Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }

    try {
      registerAndVerifySchema(
          restApp.restClient,
          getRandomYangSchemas(1).get(0),
          Collections.singletonList(new SchemaReference("bad", "bad", 100)),
          1,
          subject1);
      fail("Registering bad reference should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals("Invalid schema", Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }

    // test getAllSubjects with existing data
    assertEquals(
        "Getting all subjects should match all registered subjects",
        allSubjects,
        restApp.restClient.getAllSubjects());
  }

  static final String RefYangSchema =
      "module ref {\n"
          + "  yang-version \"1.1\";\n"
          + "  namespace \"urn:example:schema:test:ref\";\n"
          + "  prefix \"ref\";\n"
          + "  revision \"2023-02-06\";\n"
          + "  typedef aType {\n"
          + "    type \"int8\";\n"
          + "  }\n"
          + "}\n";

  static final String RootYangSchema =
      "module root {\n"
          + "  yang-version \"1.1\";\n"
          + "  namespace \"urn:example:schema:test:root\";\n"
          + "  prefix \"root\";\n"
          + "  import ref {\n"
          + "    prefix \"ref\";\n"
          + "    revision-date \"2023-02-06\";\n"
          + "  }\n"
          + "  revision \"2023-02-06\";\n"
          + "  container root {\n"
          + "    leaf testLeaf {\n"
          + "      type \"ref:aType\";\n"
          + "      description\n"
          + "        \"Example leaf\";\n"
          + "    }\n"
          + "  }\n"
          + "}\n";

  public static Map<String, String> getYangSchemaWithDependencies() {
    Map<String, String> schemas = new HashMap<>();
    schemas.put("ref", RefYangSchema);
    schemas.put("root", RootYangSchema);
    return schemas;
  }

  public static String getBadSchema() {
    return "module bad {\n";
  }
}
