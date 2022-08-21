/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  java.lang.IllegalArgumentException
 *  java.lang.Integer
 *  java.lang.Iterable
 *  java.lang.Long
 *  java.lang.Object
 *  java.lang.String
 *  java.util.Objects
 *  org.apache.kafka.common.header.Header
 *  org.apache.kafka.common.header.Headers
 *  org.apache.kafka.common.header.internals.RecordHeaders
 */
package org.apache.kafka.clients.producer;

import java.util.Objects;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public class ProducerRecord<K, V> {
    private final String topic;
    private final Integer partition;
    private final Headers headers;
    private final K key;
    private final V value;
    private final Long timestamp;

    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic cannot be null.");
        }
        if (timestamp != null && timestamp < 0L) {
            throw new IllegalArgumentException(String.format((String)"Invalid timestamp: %d. Timestamp should always be non-negative or null.", (Object[])new Object[]{timestamp}));
        }
        if (partition != null && partition < 0) {
            throw new IllegalArgumentException(String.format((String)"Invalid partition: %d. Partition number should always be non-negative or null.", (Object[])new Object[]{partition}));
        }
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = new RecordHeaders(headers);
    }

    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
        this(topic, partition, timestamp, key, value, null);
    }

    public ProducerRecord(String topic, Integer partition, K key, V value, Iterable<Header> headers) {
        this(topic, partition, null, key, value, headers);
    }

    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this(topic, partition, null, key, value, null);
    }

    public ProducerRecord(String topic, K key, V value) {
        this(topic, null, null, key, value, null);
    }

    public ProducerRecord(String topic, V value) {
        this(topic, null, null, null, value, null);
    }

    public String topic() {
        return this.topic;
    }

    public Headers headers() {
        return this.headers;
    }

    public K key() {
        return this.key;
    }

    public V value() {
        return this.value;
    }

    public Long timestamp() {
        return this.timestamp;
    }

    public Integer partition() {
        return this.partition;
    }

    public String toString() {
        String headers = this.headers == null ? "null" : this.headers.toString();
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "ProducerRecord(topic=" + this.topic + ", partition=" + this.partition + ", headers=" + headers + ", key=" + key + ", value=" + value + ", timestamp=" + timestamp + ")";
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProducerRecord)) {
            return false;
        }
        ProducerRecord that = (ProducerRecord)o;
        return Objects.equals(this.key, that.key) && Objects.equals((Object)this.partition, (Object)that.partition) && Objects.equals((Object)this.topic, (Object)that.topic) && Objects.equals((Object)this.headers, (Object)that.headers) && Objects.equals(this.value, that.value) && Objects.equals((Object)this.timestamp, (Object)that.timestamp);
    }

    public int hashCode() {
        int result = this.topic != null ? this.topic.hashCode() : 0;
        result = 31 * result + (this.partition != null ? this.partition.hashCode() : 0);
        result = 31 * result + (this.headers != null ? this.headers.hashCode() : 0);
        result = 31 * result + (this.key != null ? this.key.hashCode() : 0);
        result = 31 * result + (this.value != null ? this.value.hashCode() : 0);
        result = 31 * result + (this.timestamp != null ? this.timestamp.hashCode() : 0);
        return result;
    }
}
