/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

