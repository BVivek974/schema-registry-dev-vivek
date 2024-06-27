package com.insa.kafka.serializers.yang.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;

public class YangJsonRecord {

    private final JsonNode data;
    private final YangSchemaContext schema;
    private final String schemaString;


    public YangJsonRecord(JsonNode data, YangSchemaContext schema, String schemaString) {
        this.data = data;
        this.schema = schema;
        this.schemaString = schemaString;
    }

    public YangSchemaContext getSchema() {
        return schema;
    }

    public JsonNode getData() {
        return data;
    }

    public String getSchemaString() {
        return schemaString;
    }
}
