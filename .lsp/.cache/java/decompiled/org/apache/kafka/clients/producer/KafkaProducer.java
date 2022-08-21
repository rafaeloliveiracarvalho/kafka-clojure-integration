/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  java.lang.AutoCloseable
 *  java.lang.ClassCastException
 *  java.lang.Deprecated
 *  java.lang.Exception
 *  java.lang.IllegalArgumentException
 *  java.lang.IllegalStateException
 *  java.lang.Integer
 *  java.lang.InterruptedException
 *  java.lang.Long
 *  java.lang.Math
 *  java.lang.Object
 *  java.lang.Runnable
 *  java.lang.RuntimeException
 *  java.lang.Short
 *  java.lang.String
 *  java.lang.Thread
 *  java.lang.Throwable
 *  java.time.Duration
 *  java.util.Collections
 *  java.util.List
 *  java.util.Map
 *  java.util.Objects
 *  java.util.Properties
 *  java.util.concurrent.Future
 *  java.util.concurrent.TimeUnit
 *  java.util.concurrent.atomic.AtomicReference
 *  org.apache.kafka.clients.ApiVersions
 *  org.apache.kafka.clients.ClientUtils
 *  org.apache.kafka.clients.KafkaClient
 *  org.apache.kafka.clients.Metadata
 *  org.apache.kafka.clients.NetworkClient
 *  org.apache.kafka.clients.consumer.ConsumerGroupMetadata
 *  org.apache.kafka.clients.consumer.OffsetAndMetadata
 *  org.apache.kafka.clients.producer.Callback
 *  org.apache.kafka.clients.producer.KafkaProducer$ClusterAndWaitTime
 *  org.apache.kafka.clients.producer.KafkaProducer$FutureFailure
 *  org.apache.kafka.clients.producer.KafkaProducer$InterceptorCallback
 *  org.apache.kafka.clients.producer.Partitioner
 *  org.apache.kafka.clients.producer.Producer
 *  org.apache.kafka.clients.producer.ProducerConfig
 *  org.apache.kafka.clients.producer.ProducerInterceptor
 *  org.apache.kafka.clients.producer.RecordMetadata
 *  org.apache.kafka.clients.producer.internals.BufferPool
 *  org.apache.kafka.clients.producer.internals.KafkaProducerMetrics
 *  org.apache.kafka.clients.producer.internals.ProducerInterceptors
 *  org.apache.kafka.clients.producer.internals.ProducerMetadata
 *  org.apache.kafka.clients.producer.internals.ProducerMetrics
 *  org.apache.kafka.clients.producer.internals.RecordAccumulator
 *  org.apache.kafka.clients.producer.internals.RecordAccumulator$RecordAppendResult
 *  org.apache.kafka.clients.producer.internals.Sender
 *  org.apache.kafka.clients.producer.internals.SenderMetricsRegistry
 *  org.apache.kafka.clients.producer.internals.TransactionManager
 *  org.apache.kafka.clients.producer.internals.TransactionalRequestResult
 *  org.apache.kafka.common.Cluster
 *  org.apache.kafka.common.KafkaException
 *  org.apache.kafka.common.Metric
 *  org.apache.kafka.common.MetricName
 *  org.apache.kafka.common.PartitionInfo
 *  org.apache.kafka.common.TopicPartition
 *  org.apache.kafka.common.config.AbstractConfig
 *  org.apache.kafka.common.config.ConfigException
 *  org.apache.kafka.common.errors.ApiException
 *  org.apache.kafka.common.errors.InterruptException
 *  org.apache.kafka.common.errors.InvalidTopicException
 *  org.apache.kafka.common.errors.ProducerFencedException
 *  org.apache.kafka.common.errors.RecordTooLargeException
 *  org.apache.kafka.common.errors.SerializationException
 *  org.apache.kafka.common.errors.TimeoutException
 *  org.apache.kafka.common.header.Header
 *  org.apache.kafka.common.header.Headers
 *  org.apache.kafka.common.header.internals.RecordHeaders
 *  org.apache.kafka.common.internals.ClusterResourceListeners
 *  org.apache.kafka.common.metrics.JmxReporter
 *  org.apache.kafka.common.metrics.KafkaMetricsContext
 *  org.apache.kafka.common.metrics.MetricConfig
 *  org.apache.kafka.common.metrics.Metrics
 *  org.apache.kafka.common.metrics.MetricsContext
 *  org.apache.kafka.common.metrics.MetricsReporter
 *  org.apache.kafka.common.metrics.Sensor
 *  org.apache.kafka.common.metrics.Sensor$RecordingLevel
 *  org.apache.kafka.common.network.ChannelBuilder
 *  org.apache.kafka.common.network.Selectable
 *  org.apache.kafka.common.network.Selector
 *  org.apache.kafka.common.record.AbstractRecords
 *  org.apache.kafka.common.record.CompressionType
 *  org.apache.kafka.common.serialization.Serializer
 *  org.apache.kafka.common.utils.AppInfoParser
 *  org.apache.kafka.common.utils.KafkaThread
 *  org.apache.kafka.common.utils.LogContext
 *  org.apache.kafka.common.utils.Time
 *  org.apache.kafka.common.utils.Utils
 *  org.slf4j.Logger
 */
package org.apache.kafka.clients.producer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.KafkaProducerMetrics;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.SenderMetricsRegistry;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

public class KafkaProducer<K, V>
implements Producer<K, V> {
    private final Logger log;
    private static final String JMX_PREFIX = "kafka.producer";
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";
    public static final String PRODUCER_METRIC_GROUP_NAME = "producer-metrics";
    private final String clientId;
    final Metrics metrics;
    private final KafkaProducerMetrics producerMetrics;
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long totalMemorySize;
    private final ProducerMetadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Thread ioThread;
    private final CompressionType compressionType;
    private final Sensor errors;
    private final Time time;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    private final ProducerInterceptors<K, V> interceptors;
    private final ApiVersions apiVersions;
    private final TransactionManager transactionManager;

    public KafkaProducer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer)), keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
    }

    public KafkaProducer(Properties properties) {
        this(properties, null, null);
    }

    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this((Map<String, Object>)Utils.propsToMap((Properties)properties), keySerializer, valueSerializer);
    }

    KafkaProducer(ProducerConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer, ProducerMetadata metadata, KafkaClient kafkaClient, ProducerInterceptors<K, V> interceptors, Time time) {
        try {
            this.producerConfig = config;
            this.time = time;
            String transactionalId = config.getString("transactional.id");
            this.clientId = config.getString("client.id");
            LogContext logContext = transactionalId == null ? new LogContext(String.format((String)"[Producer clientId=%s] ", (Object[])new Object[]{this.clientId})) : new LogContext(String.format((String)"[Producer clientId=%s, transactionalId=%s] ", (Object[])new Object[]{this.clientId, transactionalId}));
            this.log = logContext.logger(KafkaProducer.class);
            this.log.trace("Starting the Kafka producer");
            Map metricTags = Collections.singletonMap((Object)"client-id", (Object)this.clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt("metrics.num.samples").intValue()).timeWindow(config.getLong("metrics.sample.window.ms").longValue(), TimeUnit.MILLISECONDS).recordLevel(Sensor.RecordingLevel.forName((String)config.getString("metrics.recording.level"))).tags(metricTags);
            List reporters = config.getConfiguredInstances("metric.reporters", MetricsReporter.class, Collections.singletonMap((Object)"client.id", (Object)this.clientId));
            JmxReporter jmxReporter = new JmxReporter();
            jmxReporter.configure(config.originals(Collections.singletonMap((Object)"client.id", (Object)this.clientId)));
            reporters.add((Object)jmxReporter);
            KafkaMetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX, config.originalsWithPrefix("metrics.context."));
            this.metrics = new Metrics(metricConfig, reporters, time, (MetricsContext)metricsContext);
            this.producerMetrics = new KafkaProducerMetrics(this.metrics);
            this.partitioner = (Partitioner)config.getConfiguredInstance("partitioner.class", Partitioner.class, Collections.singletonMap((Object)"client.id", (Object)this.clientId));
            long retryBackoffMs = config.getLong("retry.backoff.ms");
            if (keySerializer == null) {
                this.keySerializer = (Serializer)config.getConfiguredInstance("key.serializer", Serializer.class);
                this.keySerializer.configure(config.originals(Collections.singletonMap((Object)"client.id", (Object)this.clientId)), true);
            } else {
                config.ignore("key.serializer");
                this.keySerializer = keySerializer;
            }
            if (valueSerializer == null) {
                this.valueSerializer = (Serializer)config.getConfiguredInstance("value.serializer", Serializer.class);
                this.valueSerializer.configure(config.originals(Collections.singletonMap((Object)"client.id", (Object)this.clientId)), false);
            } else {
                config.ignore("value.serializer");
                this.valueSerializer = valueSerializer;
            }
            List interceptorList = config.getConfiguredInstances("interceptor.classes", ProducerInterceptor.class, Collections.singletonMap((Object)"client.id", (Object)this.clientId));
            this.interceptors = interceptors != null ? interceptors : new ProducerInterceptors(interceptorList);
            ClusterResourceListeners clusterResourceListeners = this.configureClusterResourceListeners(keySerializer, valueSerializer, interceptorList, reporters);
            this.maxRequestSize = config.getInt("max.request.size");
            this.totalMemorySize = config.getLong("buffer.memory");
            this.compressionType = CompressionType.forName((String)config.getString("compression.type"));
            this.maxBlockTimeMs = config.getLong("max.block.ms");
            int deliveryTimeoutMs = KafkaProducer.configureDeliveryTimeout(config, this.log);
            this.apiVersions = new ApiVersions();
            this.transactionManager = this.configureTransactionState(config, logContext);
            this.accumulator = new RecordAccumulator(logContext, config.getInt("batch.size").intValue(), this.compressionType, KafkaProducer.lingerMs(config), retryBackoffMs, deliveryTimeoutMs, this.metrics, PRODUCER_METRIC_GROUP_NAME, time, this.apiVersions, this.transactionManager, new BufferPool(this.totalMemorySize, config.getInt("batch.size").intValue(), this.metrics, time, PRODUCER_METRIC_GROUP_NAME));
            List addresses = ClientUtils.parseAndValidateAddresses((List)config.getList("bootstrap.servers"), (String)config.getString("client.dns.lookup"));
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                this.metadata = new ProducerMetadata(retryBackoffMs, config.getLong("metadata.max.age.ms").longValue(), config.getLong("metadata.max.idle.ms").longValue(), logContext, clusterResourceListeners, Time.SYSTEM);
                this.metadata.bootstrap(addresses);
            }
            this.errors = this.metrics.sensor("errors");
            this.sender = this.newSender(logContext, kafkaClient, this.metadata);
            String ioThreadName = "kafka-producer-network-thread | " + this.clientId;
            this.ioThread = new KafkaThread(ioThreadName, (Runnable)this.sender, true);
            this.ioThread.start();
            config.logUnused();
            AppInfoParser.registerAppInfo((String)JMX_PREFIX, (String)this.clientId, (Metrics)this.metrics, (long)time.milliseconds());
            this.log.debug("Kafka producer started");
        }
        catch (Throwable t) {
            this.close(Duration.ofMillis((long)0L), true);
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    KafkaProducer(ProducerConfig config, LogContext logContext, Metrics metrics, Serializer<K> keySerializer, Serializer<V> valueSerializer, ProducerMetadata metadata, RecordAccumulator accumulator, TransactionManager transactionManager, Sender sender, ProducerInterceptors<K, V> interceptors, Partitioner partitioner, Time time, KafkaThread ioThread) {
        this.producerConfig = config;
        this.time = time;
        this.clientId = config.getString("client.id");
        this.log = logContext.logger(KafkaProducer.class);
        this.metrics = metrics;
        this.producerMetrics = new KafkaProducerMetrics(metrics);
        this.partitioner = partitioner;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.interceptors = interceptors;
        this.maxRequestSize = config.getInt("max.request.size");
        this.totalMemorySize = config.getLong("buffer.memory");
        this.compressionType = CompressionType.forName((String)config.getString("compression.type"));
        this.maxBlockTimeMs = config.getLong("max.block.ms");
        this.apiVersions = new ApiVersions();
        this.transactionManager = transactionManager;
        this.accumulator = accumulator;
        this.errors = this.metrics.sensor("errors");
        this.metadata = metadata;
        this.sender = sender;
        this.ioThread = ioThread;
    }

    Sender newSender(LogContext logContext, KafkaClient kafkaClient, ProducerMetadata metadata) {
        int maxInflightRequests = this.producerConfig.getInt("max.in.flight.requests.per.connection");
        int requestTimeoutMs = this.producerConfig.getInt("request.timeout.ms");
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder((AbstractConfig)this.producerConfig, (Time)this.time, (LogContext)logContext);
        ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
        Sensor throttleTimeSensor = Sender.throttleTimeSensor((SenderMetricsRegistry)metricsRegistry.senderMetrics);
        KafkaClient client = kafkaClient != null ? kafkaClient : new NetworkClient((Selectable)new Selector(this.producerConfig.getLong("connections.max.idle.ms").longValue(), this.metrics, this.time, "producer", channelBuilder, logContext), (Metadata)metadata, this.clientId, maxInflightRequests, this.producerConfig.getLong("reconnect.backoff.ms").longValue(), this.producerConfig.getLong("reconnect.backoff.max.ms").longValue(), this.producerConfig.getInt("send.buffer.bytes").intValue(), this.producerConfig.getInt("receive.buffer.bytes").intValue(), requestTimeoutMs, this.producerConfig.getLong("socket.connection.setup.timeout.ms").longValue(), this.producerConfig.getLong("socket.connection.setup.timeout.max.ms").longValue(), this.time, true, this.apiVersions, throttleTimeSensor, logContext);
        short acks = Short.parseShort((String)this.producerConfig.getString("acks"));
        return new Sender(logContext, client, metadata, this.accumulator, maxInflightRequests == 1, this.producerConfig.getInt("max.request.size").intValue(), acks, this.producerConfig.getInt("retries").intValue(), metricsRegistry.senderMetrics, this.time, requestTimeoutMs, this.producerConfig.getLong("retry.backoff.ms").longValue(), this.transactionManager, this.apiVersions);
    }

    private static int lingerMs(ProducerConfig config) {
        return (int)Math.min((long)config.getLong("linger.ms"), (long)Integer.MAX_VALUE);
    }

    private static int configureDeliveryTimeout(ProducerConfig config, Logger log) {
        int requestTimeoutMs;
        int lingerMs;
        int lingerAndRequestTimeoutMs;
        int deliveryTimeoutMs = config.getInt("delivery.timeout.ms");
        if (deliveryTimeoutMs < (lingerAndRequestTimeoutMs = (int)Math.min((long)((long)(lingerMs = KafkaProducer.lingerMs(config)) + (long)(requestTimeoutMs = config.getInt("request.timeout.ms").intValue())), (long)Integer.MAX_VALUE))) {
            if (config.originals().containsKey((Object)"delivery.timeout.ms")) {
                throw new ConfigException("delivery.timeout.ms should be equal to or larger than linger.ms + request.timeout.ms");
            }
            deliveryTimeoutMs = lingerAndRequestTimeoutMs;
            log.warn("{} should be equal to or larger than {} + {}. Setting it to {}.", new Object[]{"delivery.timeout.ms", "linger.ms", "request.timeout.ms", deliveryTimeoutMs});
        }
        return deliveryTimeoutMs;
    }

    private TransactionManager configureTransactionState(ProducerConfig config, LogContext logContext) {
        TransactionManager transactionManager = null;
        if (config.getBoolean("enable.idempotence").booleanValue()) {
            long retryBackoffMs;
            int transactionTimeoutMs;
            String transactionalId = config.getString("transactional.id");
            transactionManager = new TransactionManager(logContext, transactionalId, transactionTimeoutMs = config.getInt("transaction.timeout.ms").intValue(), retryBackoffMs = config.getLong("retry.backoff.ms").longValue(), this.apiVersions);
            if (transactionManager.isTransactional()) {
                this.log.info("Instantiated a transactional producer.");
            } else {
                this.log.info("Instantiated an idempotent producer.");
            }
        }
        return transactionManager;
    }

    public void initTransactions() {
        this.throwIfNoTransactionManager();
        this.throwIfProducerClosed();
        long now = this.time.nanoseconds();
        TransactionalRequestResult result = this.transactionManager.initializeTransactions();
        this.sender.wakeup();
        result.await(this.maxBlockTimeMs, TimeUnit.MILLISECONDS);
        this.producerMetrics.recordInit(this.time.nanoseconds() - now);
    }

    public void beginTransaction() throws ProducerFencedException {
        this.throwIfNoTransactionManager();
        this.throwIfProducerClosed();
        long now = this.time.nanoseconds();
        this.transactionManager.beginTransaction();
        this.producerMetrics.recordBeginTxn(this.time.nanoseconds() - now);
    }

    @Deprecated
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
        this.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));
    }

    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        this.throwIfInvalidGroupMetadata(groupMetadata);
        this.throwIfNoTransactionManager();
        this.throwIfProducerClosed();
        if (!offsets.isEmpty()) {
            long start = this.time.nanoseconds();
            TransactionalRequestResult result = this.transactionManager.sendOffsetsToTransaction(offsets, groupMetadata);
            this.sender.wakeup();
            result.await(this.maxBlockTimeMs, TimeUnit.MILLISECONDS);
            this.producerMetrics.recordSendOffsets(this.time.nanoseconds() - start);
        }
    }

    public void commitTransaction() throws ProducerFencedException {
        this.throwIfNoTransactionManager();
        this.throwIfProducerClosed();
        long commitStart = this.time.nanoseconds();
        TransactionalRequestResult result = this.transactionManager.beginCommit();
        this.sender.wakeup();
        result.await(this.maxBlockTimeMs, TimeUnit.MILLISECONDS);
        this.producerMetrics.recordCommitTxn(this.time.nanoseconds() - commitStart);
    }

    public void abortTransaction() throws ProducerFencedException {
        this.throwIfNoTransactionManager();
        this.throwIfProducerClosed();
        this.log.info("Aborting incomplete transaction");
        long abortStart = this.time.nanoseconds();
        TransactionalRequestResult result = this.transactionManager.beginAbort();
        this.sender.wakeup();
        result.await(this.maxBlockTimeMs, TimeUnit.MILLISECONDS);
        this.producerMetrics.recordAbortTxn(this.time.nanoseconds() - abortStart);
    }

    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return this.send(record, null);
    }

    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        ProducerRecord interceptedRecord = this.interceptors.onSend(record);
        return this.doSend(interceptedRecord, callback);
    }

    private void throwIfProducerClosed() {
        if (this.sender == null || !this.sender.isRunning()) {
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
        }
    }

    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            long timestamp;
            byte[] serializedValue;
            byte[] serializedKey;
            ClusterAndWaitTime clusterAndWaitTime;
            this.throwIfProducerClosed();
            long nowMs = this.time.milliseconds();
            try {
                clusterAndWaitTime = this.waitOnMetadata(record.topic(), record.partition(), nowMs, this.maxBlockTimeMs);
            }
            catch (KafkaException e) {
                if (this.metadata.isClosed()) {
                    throw new KafkaException("Producer closed while send in progress", (Throwable)e);
                }
                throw e;
            }
            nowMs += clusterAndWaitTime.waitedOnMetadataMs;
            long remainingWaitMs = Math.max((long)0L, (long)(this.maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs));
            Cluster cluster = clusterAndWaitTime.cluster;
            try {
                serializedKey = this.keySerializer.serialize(record.topic(), record.headers(), record.key());
            }
            catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() + " to class " + this.producerConfig.getClass("key.serializer").getName() + " specified in key.serializer", (Throwable)cce);
            }
            try {
                serializedValue = this.valueSerializer.serialize(record.topic(), record.headers(), record.value());
            }
            catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() + " to class " + this.producerConfig.getClass("value.serializer").getName() + " specified in value.serializer", (Throwable)cce);
            }
            int partition = this.partition(record, serializedKey, serializedValue, cluster);
            tp = new TopicPartition(record.topic(), partition);
            this.setReadOnly(record.headers());
            Header[] headers = record.headers().toArray();
            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound((byte)this.apiVersions.maxUsableProduceMagic(), (CompressionType)this.compressionType, (byte[])serializedKey, (byte[])serializedValue, (Header[])headers);
            this.ensureValidRecordSize(serializedSize);
            long l = timestamp = record.timestamp() == null ? nowMs : record.timestamp();
            if (this.log.isTraceEnabled()) {
                this.log.trace("Attempting to append record {} with callback {} to topic {} partition {}", new Object[]{record, callback, record.topic(), partition});
            }
            InterceptorCallback interceptCallback = new InterceptorCallback(callback, this.interceptors, tp, null);
            RecordAccumulator.RecordAppendResult result = this.accumulator.append(tp, timestamp, serializedKey, serializedValue, headers, (Callback)interceptCallback, remainingWaitMs, true, nowMs);
            if (result.abortForNewBatch) {
                int prevPartition = partition;
                this.partitioner.onNewBatch(record.topic(), cluster, prevPartition);
                partition = this.partition(record, serializedKey, serializedValue, cluster);
                tp = new TopicPartition(record.topic(), partition);
                if (this.log.isTraceEnabled()) {
                    this.log.trace("Retrying append due to new batch creation for topic {} partition {}. The old partition was {}", new Object[]{record.topic(), partition, prevPartition});
                }
                interceptCallback = new InterceptorCallback(callback, this.interceptors, tp, null);
                result = this.accumulator.append(tp, timestamp, serializedKey, serializedValue, headers, (Callback)interceptCallback, remainingWaitMs, false, nowMs);
            }
            if (this.transactionManager != null) {
                this.transactionManager.maybeAddPartition(tp);
            }
            if (result.batchIsFull || result.newBatchCreated) {
                this.log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", (Object)record.topic(), (Object)partition);
                this.sender.wakeup();
            }
            return result.future;
        }
        catch (ApiException e) {
            this.log.debug("Exception occurred during message send:", (Throwable)e);
            if (tp == null) {
                tp = ProducerInterceptors.extractTopicPartition(record);
            }
            if (callback != null) {
                RecordMetadata nullMetadata = new RecordMetadata(tp, -1L, -1, -1L, -1, -1);
                callback.onCompletion(nullMetadata, (Exception)((Object)e));
            }
            this.errors.record();
            this.interceptors.onSendError(record, tp, (Exception)((Object)e));
            if (this.transactionManager != null) {
                this.transactionManager.maybeTransitionToErrorState((RuntimeException)e);
            }
            return new FutureFailure((Exception)((Object)e));
        }
        catch (InterruptedException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, (Exception)((Object)e));
            throw new InterruptException(e);
        }
        catch (KafkaException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, (Exception)((Object)e));
            throw e;
        }
        catch (Exception e) {
            this.interceptors.onSendError(record, tp, e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders)headers).setReadOnly();
        }
    }

    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long nowMs, long maxWaitMs) throws InterruptedException {
        Cluster cluster = this.metadata.fetch();
        if (cluster.invalidTopics().contains((Object)topic)) {
            throw new InvalidTopicException(topic);
        }
        this.metadata.add(topic, nowMs);
        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        if (partitionsCount != null && (partition == null || partition < partitionsCount)) {
            return new ClusterAndWaitTime(cluster, 0L);
        }
        long remainingWaitMs = maxWaitMs;
        long elapsed = 0L;
        do {
            if (partition != null) {
                this.log.trace("Requesting metadata update for partition {} of topic {}.", (Object)partition, (Object)topic);
            } else {
                this.log.trace("Requesting metadata update for topic {}.", (Object)topic);
            }
            this.metadata.add(topic, nowMs + elapsed);
            int version = this.metadata.requestUpdateForTopic(topic);
            this.sender.wakeup();
            try {
                this.metadata.awaitUpdate(version, remainingWaitMs);
            }
            catch (TimeoutException ex) {
                throw new TimeoutException(String.format((String)"Topic %s not present in metadata after %d ms.", (Object[])new Object[]{topic, maxWaitMs}));
            }
            cluster = this.metadata.fetch();
            elapsed = this.time.milliseconds() - nowMs;
            if (elapsed >= maxWaitMs) {
                throw new TimeoutException(partitionsCount == null ? String.format((String)"Topic %s not present in metadata after %d ms.", (Object[])new Object[]{topic, maxWaitMs}) : String.format((String)"Partition %d of topic %s with partition count %d is not present in metadata after %d ms.", (Object[])new Object[]{partition, topic, partitionsCount, maxWaitMs}));
            }
            this.metadata.maybeThrowExceptionForTopic(topic);
            remainingWaitMs = maxWaitMs - elapsed;
        } while ((partitionsCount = cluster.partitionCountForTopic(topic)) == null || partition != null && partition >= partitionsCount);
        return new ClusterAndWaitTime(cluster, elapsed);
    }

    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize) {
            throw new RecordTooLargeException("The message is " + size + " bytes when serialized which is larger than " + this.maxRequestSize + ", which is the value of the " + "max.request.size" + " configuration.");
        }
        if ((long)size > this.totalMemorySize) {
            throw new RecordTooLargeException("The message is " + size + " bytes when serialized which is larger than the total memory buffer you have configured with the " + "buffer.memory" + " configuration.");
        }
    }

    public void flush() {
        this.log.trace("Flushing accumulated records in producer.");
        long start = this.time.nanoseconds();
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        }
        catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
        finally {
            this.producerMetrics.recordFlush(this.time.nanoseconds() - start);
        }
    }

    public List<PartitionInfo> partitionsFor(String topic) {
        Objects.requireNonNull((Object)topic, (String)"topic cannot be null");
        try {
            return this.waitOnMetadata((String)topic, null, (long)this.time.milliseconds(), (long)this.maxBlockTimeMs).cluster.partitionsForTopic(topic);
        }
        catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap((Map)this.metrics.metrics());
    }

    public void close() {
        this.close(Duration.ofMillis((long)Long.MAX_VALUE));
    }

    public void close(Duration timeout) {
        this.close(timeout, false);
    }

    private void close(Duration timeout, boolean swallowException) {
        boolean invokedFromCallback;
        long timeoutMs = timeout.toMillis();
        if (timeoutMs < 0L) {
            throw new IllegalArgumentException("The timeout cannot be negative.");
        }
        this.log.info("Closing the Kafka producer with timeoutMillis = {} ms.", (Object)timeoutMs);
        AtomicReference firstException = new AtomicReference();
        boolean bl = invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeoutMs > 0L) {
            if (invokedFromCallback) {
                this.log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.", (Object)timeoutMs);
            } else {
                if (this.sender != null) {
                    this.sender.initiateClose();
                }
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeoutMs);
                    }
                    catch (InterruptedException t) {
                        firstException.compareAndSet(null, (Object)new InterruptException(t));
                        this.log.error("Interrupted while joining ioThread", (Throwable)t);
                    }
                }
            }
        }
        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            this.log.info("Proceeding to force close the producer since pending requests could not be completed within timeout {} ms.", (Object)timeoutMs);
            this.sender.forceClose();
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                }
                catch (InterruptedException e) {
                    firstException.compareAndSet(null, (Object)new InterruptException(e));
                }
            }
        }
        Utils.closeQuietly(this.interceptors, (String)"producer interceptors", (AtomicReference)firstException);
        Utils.closeQuietly((AutoCloseable)this.producerMetrics, (String)"producer metrics wrapper", (AtomicReference)firstException);
        Utils.closeQuietly((AutoCloseable)this.metrics, (String)"producer metrics", (AtomicReference)firstException);
        Utils.closeQuietly(this.keySerializer, (String)"producer keySerializer", (AtomicReference)firstException);
        Utils.closeQuietly(this.valueSerializer, (String)"producer valueSerializer", (AtomicReference)firstException);
        Utils.closeQuietly((AutoCloseable)this.partitioner, (String)"producer partitioner", (AtomicReference)firstException);
        AppInfoParser.unregisterAppInfo((String)JMX_PREFIX, (String)this.clientId, (Metrics)this.metrics);
        Throwable exception = (Throwable)((Object)firstException.get());
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException)exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
        this.log.debug("Kafka producer has been closed");
    }

    private ClusterResourceListeners configureClusterResourceListeners(Serializer<K> keySerializer, Serializer<V> valueSerializer, List<?> ... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList : candidateLists) {
            clusterResourceListeners.maybeAddAll(candidateList);
        }
        clusterResourceListeners.maybeAdd(keySerializer);
        clusterResourceListeners.maybeAdd(valueSerializer);
        return clusterResourceListeners;
    }

    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        if (partition != null) {
            return partition;
        }
        int customPartition = this.partitioner.partition(record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
        if (customPartition < 0) {
            throw new IllegalArgumentException(String.format((String)"The partitioner generated an invalid partition number: %d. Partition number should always be non-negative.", (Object[])new Object[]{customPartition}));
        }
        return customPartition;
    }

    private void throwIfInvalidGroupMetadata(ConsumerGroupMetadata groupMetadata) {
        if (groupMetadata == null) {
            throw new IllegalArgumentException("Consumer group metadata could not be null");
        }
        if (groupMetadata.generationId() > 0 && "".equals((Object)groupMetadata.memberId())) {
            throw new IllegalArgumentException("Passed in group metadata " + groupMetadata + " has generationId > 0 but member.id ");
        }
    }

    private void throwIfNoTransactionManager() {
        if (this.transactionManager == null) {
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions by setting the transactional.id configuration property");
        }
    }

    String getClientId() {
        return this.clientId;
    }
}
