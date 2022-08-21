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
 *  org.apache.kafka.common.serialization.Serializer
 */
package org.apache.kafka.common.serialization;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class StringSerializer
implements Serializer<String> {
    private String encoding = StandardCharsets.UTF_8.name();

    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get((Object)propertyName);
        if (encodingValue == null) {
            encodingValue = configs.get((Object)"serializer.encoding");
        }
        if (encodingValue instanceof String) {
            this.encoding = (String)encodingValue;
        }
    }

    public byte[] serialize(String topic, String data) {
        try {
            if (data == null) {
                return null;
            }
            return data.getBytes(this.encoding);
        }
        catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + this.encoding);
        }
    }
}
