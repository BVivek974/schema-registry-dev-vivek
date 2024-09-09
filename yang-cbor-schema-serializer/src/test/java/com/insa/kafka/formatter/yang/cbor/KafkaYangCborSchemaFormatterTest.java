package com.insa.kafka.formatter.yang.cbor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.insa.kafka.serializers.yang.cbor.KafkaYangCborSchemaDeserializerConfig;
import com.insa.kafka.serializers.yang.cbor.KafkaYangCborSchemaSerializerTest;
import com.swisscom.kafka.schemaregistry.yang.YangSchema;
import com.swisscom.kafka.schemaregistry.yang.YangSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.TimestampType;
import org.dom4j.DocumentException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.parser.YangParserException;
import org.yangcentral.yangkit.parser.YangYinParser;

import java.io.*;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class KafkaYangCborSchemaFormatterTest {

  private static Properties props;
  private static YangCborSchemaMessageFormatter formatter;
  private static YangSchema recordSchema = null;
  private static String url = "mock://test";
  private static ObjectMapper objectMapper = new ObjectMapper();
  private static SchemaRegistryClient schemaRegistry = null;

  @BeforeAll
  public static void setUp() {
    props = new Properties();
    props.put(KafkaYangCborSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, url);
    formatter = new YangCborSchemaMessageFormatter();
    formatter.init(props);
    schemaRegistry = new MockSchemaRegistryClient(Collections.singletonList(new YangSchemaProvider()));
    YangSchemaContext schemaContext;
    String schemaString;
    try {
      schemaContext = YangYinParser.parse(KafkaYangCborSchemaSerializerTest.class.getClassLoader().getResource("formatter/yangs/test.yang").getFile());
      schemaContext.validate();
      schemaString = schemaContext.getModules().get(0).getOriginalString();
    } catch (DocumentException | IOException | YangParserException e) {
      throw new RuntimeException(e);
    }
    recordSchema = (YangSchema) schemaRegistry.parseSchema(YangSchema.TYPE, schemaString, Collections.emptyList()).get();
  }

  @AfterAll
  public static void tearDown() {
    MockSchemaRegistry.dropScope("test");
  }

  @Test
  public void testKafkaYangCborJsonValueFormatter() throws JsonProcessingException {
    String input = "{\"data\":{\"insa-test:insa-container\":{\"d\": \"test\"}}}";

    BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(input.getBytes())));
    YangCborSchemaMessageReader yangJsonSchemaMessageReader = new YangCborSchemaMessageReader(url, null, recordSchema, "topic1", false, reader,
        false, true, false);

    ProducerRecord<byte[], byte[]> message = yangJsonSchemaMessageReader.readMessage();
    byte[] serializedValue = message.value();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);

    ConsumerRecord<byte[], byte[]> crecord = new ConsumerRecord<>("topic1", 0, 200, 1000, TimestampType.LOG_APPEND_TIME, 0, 0, serializedValue.length,
        null, serializedValue);

    formatter.writeTo(crecord, ps);

    String output = baos.toString();

    assertEquals(objectMapper.readTree(input), objectMapper.readTree(output));

  }

  @Test
  public void testInvalidFormat() {
    String input = "{\"data\":{\"insa-test:insa-container\":{\"d\": \"test\"";

    BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(input.getBytes())));
    YangCborSchemaMessageReader yangJsonSchemaMessageReader = new YangCborSchemaMessageReader(url, null, recordSchema, "topic1", false, reader,
        false, true, false);

    assertThrowsExactly(SerializationException.class, yangJsonSchemaMessageReader::readMessage);
  }

}
