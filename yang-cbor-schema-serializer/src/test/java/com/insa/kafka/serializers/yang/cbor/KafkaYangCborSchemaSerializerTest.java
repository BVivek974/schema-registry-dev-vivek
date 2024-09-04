package com.insa.kafka.serializers.yang.cbor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.swisscom.kafka.schemaregistry.yang.YangSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.common.errors.SerializationException;
import org.dom4j.DocumentException;
import org.junit.jupiter.api.Test;
import org.yangcentral.yangkit.common.api.validate.ValidatorResultBuilder;
import org.yangcentral.yangkit.data.api.model.YangDataDocument;
import org.yangcentral.yangkit.data.codec.json.YangDataDocumentJsonParser;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.parser.YangParserException;
import org.yangcentral.yangkit.parser.YangYinParser;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaYangCborSchemaSerializerTest {

  private final Properties config;
  private final SchemaRegistryClient schemaRegistry;
  private KafkaYangCborSchemaSerializer serializer;
  private KafkaYangCborSchemaSerializer noValidationSerializer;
  private KafkaYangCborSchemaDeserializer deserializer;
  private final String topic;

  public KafkaYangCborSchemaSerializerTest() {
    config = new Properties();
    config.put(KafkaYangCborSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    config.put(KafkaYangCborSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config.put(KafkaYangCborSchemaSerializerConfig.YANG_CBOR_FAIL_INVALID_SCHEMA, true);

    schemaRegistry = new MockSchemaRegistryClient(Collections.singletonList(new YangSchemaProvider()));

    serializer = new KafkaYangCborSchemaSerializer(schemaRegistry);
    serializer.configure(new HashMap<>(config), true);

    deserializer = new KafkaYangCborSchemaDeserializer<>(schemaRegistry);
    deserializer.configure(new HashMap<>(config), true);

    Properties noValidationConfig = new Properties(config);
    noValidationConfig.put(KafkaYangCborSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
    noValidationConfig.put(KafkaYangCborSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config.put(KafkaYangCborSchemaSerializerConfig.YANG_CBOR_FAIL_INVALID_SCHEMA, false);

    noValidationSerializer = new KafkaYangCborSchemaSerializer(schemaRegistry);
    noValidationSerializer.configure(new HashMap<>(config), true);

    topic = "test";
  }

  private KafkaYangCborSchemaDeserializer getDeserializer() {
    KafkaYangCborSchemaDeserializer des = new KafkaYangCborSchemaDeserializer<>(schemaRegistry);
    des.configure(new HashMap<>(config), true);
    return des;
  }

  private <T> YangDataDocument getRecord(T o) {
    YangDataDocument doc = null;
    String newString = "{\"data\":{\"insa-test:insa-container\":{\"d\":" + o + "}}}";
    ObjectMapper mapper = new ObjectMapper();
    try {
      YangSchemaContext schemaContext = YangYinParser.parse(this.getClass().getClassLoader().getResource("serializer/yangs/test.yang").getFile());
      schemaContext.validate();
      JsonNode jsonNode = mapper.readTree(newString);
      doc = new YangDataDocumentJsonParser(schemaContext).parse(jsonNode, new ValidatorResultBuilder());
    } catch (DocumentException | IOException | YangParserException e) {
      throw new RuntimeException(e);
    }
    return doc;
  }

  private YangDataDocument getRecord(String yang, String json) {
    YangDataDocument doc = null;
    ObjectMapper mapper = new ObjectMapper(new CBORFactory());
    try {
      YangSchemaContext schemaContext = YangYinParser.parse(yang);
      schemaContext.validate();
      JsonNode jsonNode = mapper.readTree(new File(json));
      doc = new YangDataDocumentJsonParser(schemaContext).parse(jsonNode, new ValidatorResultBuilder());
    } catch (DocumentException | IOException | YangParserException e) {
      throw new RuntimeException(e);
    }
    return doc;
  }

  private <T> JsonNode getJsonNode(T o) {
    JsonNode jsonNode;
    String newString = "{\"data\":{\"insa-test:insa-container\":{\"d\":" + o + "}}}";
    ObjectMapper mapper = new ObjectMapper();
    try {
      jsonNode = mapper.readTree(newString);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return jsonNode;
  }

  private JsonNode getJsonNodeFromFile(String json) {
    JsonNode jsonNode;
    ObjectMapper mapper = new ObjectMapper(new CBORFactory());
    try {
      jsonNode = mapper.readTree(new File(json));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return jsonNode;
  }

  private JsonNode getJsonNode(YangDataDocument doc) {
    JsonNode jsonNode;
    ObjectMapper mapper = new ObjectMapper();
    try {
      jsonNode = mapper.readTree(doc.getDocString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return jsonNode;
  }

  @Test
  public void singleLeafTest() {
    byte[] bytes;
    YangDataDocument doc = null;

    bytes = serializer.serialize(topic, null);
    assertEquals(null, deserializer.deserialize(topic, bytes));


    doc = getRecord(true);
    bytes = serializer.serialize(topic, doc);
    assertEquals(getJsonNode(true), getJsonNode(deserializer.deserialize(topic, bytes)));

    doc = getRecord(123);
    bytes = serializer.serialize(topic, doc);
    assertEquals(getJsonNode(123), getJsonNode(deserializer.deserialize(topic, bytes)));

    doc = getRecord(1.23f);
    bytes = serializer.serialize(topic, doc);
    assertEquals(getJsonNode(1.23f), getJsonNode(deserializer.deserialize(topic, bytes)));

    doc = getRecord(123L);
    bytes = serializer.serialize(topic, doc);
    assertEquals(getJsonNode(123L), getJsonNode(deserializer.deserialize(topic, bytes)));

    doc = getRecord("\"abc\"");
    bytes = serializer.serialize(topic, doc);
    assertEquals(getJsonNode("\"abc\""), getJsonNode(deserializer.deserialize(topic, bytes)));

  }

  @Test
  public void serializeNull() {
    assertNull(serializer.serialize("foo", null));
  }

  @Test
  public void test1() {
    byte[] bytes;
    YangDataDocument doc = getRecord(
        this.getClass().getClassLoader().getResource("serializer/cbor/test1/test.yang").getFile(),
        this.getClass().getClassLoader().getResource("serializer/cbor/test1/valid.cbor").getFile());

    JsonNode jsonNode = getJsonNodeFromFile(this.getClass().getClassLoader().getResource("serializer/cbor/test1/valid.cbor").getFile());
    bytes = serializer.serialize(topic, doc);
    assertEquals(jsonNode, getJsonNode(deserializer.deserialize(topic, bytes)));
  }

  @Test
  public void test2() {
    YangDataDocument doc = getRecord(
        this.getClass().getClassLoader().getResource("serializer/cbor/test2/test.yang").getFile(),
        this.getClass().getClassLoader().getResource("serializer/cbor/test2/invalid.cbor").getFile());

    assertThrowsExactly(SerializationException.class, () -> serializer.serialize(topic, doc));
  }

  @Test
  public void test3() {
    byte[] bytes;
    YangDataDocument doc = getRecord(
        this.getClass().getClassLoader().getResource("serializer/cbor/test3/test.yang").getFile(),
        this.getClass().getClassLoader().getResource("serializer/cbor/test3/invalid.cbor").getFile());

    bytes = assertDoesNotThrow(() -> noValidationSerializer.serialize(topic, doc));
    assertThrowsExactly(SerializationException.class, () -> deserializer.deserialize(topic, bytes));
  }

  @Test
  public void test4() {
    byte[] bytes;
    YangDataDocument doc = getRecord(
        this.getClass().getClassLoader().getResource("serializer/cbor/test4/yangs").getFile(),
        this.getClass().getClassLoader().getResource("serializer/cbor/test4/valid.cbor").getFile());
    JsonNode jsonNode = getJsonNodeFromFile(this.getClass().getClassLoader().getResource("serializer/cbor/test4/valid.cbor").getFile());
    bytes = serializer.serialize(topic, doc);
    assertEquals(jsonNode, getJsonNode(deserializer.deserialize(topic, bytes)));
  }

  @Test
  public void test5() {
    YangDataDocument doc = getRecord(
        this.getClass().getClassLoader().getResource("serializer/cbor/test5/yangs").getFile(),
        this.getClass().getClassLoader().getResource("serializer/cbor/test5/invalid.cbor").getFile());

    assertThrowsExactly(SerializationException.class, () -> serializer.serialize(topic, doc));
  }

  @Test
  public void test6() {
    byte[] bytes;
    YangDataDocument doc = getRecord(
        this.getClass().getClassLoader().getResource("serializer/cbor/test6/yangs").getFile(),
        this.getClass().getClassLoader().getResource("serializer/cbor/test6/invalid.cbor").getFile());

    bytes = assertDoesNotThrow(() -> noValidationSerializer.serialize(topic, doc));
    assertThrowsExactly(SerializationException.class, () -> deserializer.deserialize(topic, bytes));
  }

  @Test
  public void test7() {
    byte[] bytes;
    YangDataDocument doc = getRecord(
        this.getClass().getClassLoader().getResource("serializer/cbor/test7/yangs").getFile(),
        this.getClass().getClassLoader().getResource("serializer/cbor/test7/valid.cbor").getFile());

    JsonNode jsonNode = getJsonNodeFromFile(this.getClass().getClassLoader().getResource("serializer/cbor/test7/valid.cbor").getFile());
    bytes = serializer.serialize(topic, doc);
    assertEquals(jsonNode, getJsonNode(deserializer.deserialize(topic, bytes)));
  }

  @Test
  public void test8() {
    byte[] bytes;
    YangDataDocument doc = getRecord(
        this.getClass().getClassLoader().getResource("serializer/cbor/test8/yangs").getFile(),
        this.getClass().getClassLoader().getResource("serializer/cbor/test8/valid.cbor").getFile());

    JsonNode jsonNode = getJsonNodeFromFile(this.getClass().getClassLoader().getResource("serializer/cbor/test8/valid.cbor").getFile());
    bytes = serializer.serialize(topic, doc);
    assertEquals(jsonNode, getJsonNode(deserializer.deserialize(topic, bytes)));
  }



}
