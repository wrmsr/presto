package com.wrmsr.presto.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class RedactedSerializer extends StdSerializer<Object> {

    public static final String DEFAULT_REDACTED_STR = "<redacted>";

    public final String redactedStr;

    public RedactedSerializer(String redactedStr) {
        super(Object.class);
        this.redactedStr = redactedStr;
    }

    public RedactedSerializer() {
        this(DEFAULT_REDACTED_STR);
    }

    @Override
    public void serialize(Object value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException {
        if (value == null)
            jgen.writeNull();
        else
            jgen.writeString(redactedStr);
    }
}

