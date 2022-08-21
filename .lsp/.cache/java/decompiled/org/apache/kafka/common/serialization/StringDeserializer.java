/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  java.io.UnsupportedEncodingException
 *  java.lang.Object
 *  java.lang.String
 *  java.nio.charset.StandardCharsets
 *  java.util.Map
 *  org.apache.kafka.common.errors.SerializationException
 *  org.apache.kafka.common.serialization.Deserializer
 */
package org.apache.kafka.common.serialization;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class StringDeserializer
implements Deserializer<String> {
    private String encoding = StandardCharsets.UTF_8.name();

    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
        Object encodingValue = configs.get((Object)propertyName);
        if (encodingValue == null) {
            encodingValue = configs.get((Object)"deserializer.encoding");
        }
        if (encodingValue instanceof String) {
            this.encoding = (String)encodingValue;
        }
    }

    public String deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            return new String(data, this.encoding);
        }
        catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + this.encoding);
        }
    }
}
