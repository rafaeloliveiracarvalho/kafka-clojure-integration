/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  java.lang.AutoCloseable
 *  java.lang.Deprecated
 *  java.lang.IllegalArgumentException
 *  java.lang.IllegalStateException
 *  java.lang.Long
 *  java.lang.Math
 *  java.lang.Object
 *  java.lang.String
 *  java.lang.Thread
 *  java.lang.Throwable
 *  java.time.Duration
 *  java.util.Collection
 *  java.util.Collections
 *  java.util.ConcurrentModificationException
 *  java.util.HashMap
 *  java.util.HashSet
 *  java.util.Iterator
 *  java.util.List
 *  java.util.Locale
 *  java.util.Map
 *  java.util.Map$Entry
 *  java.util.Objects
 *  java.util.Optional
 *  java.util.OptionalLong
 *  java.util.Properties
 *  java.util.Set
 *  java.util.concurrent.TimeUnit
 *  java.util.concurrent.atomic.AtomicInteger
 *  java.util.concurrent.atomic.AtomicLong
 *  java.util.concurrent.atomic.AtomicReference
 *  java.util.regex.Pattern
 *  org.apache.kafka.clients.ApiVersions
 *  org.apache.kafka.clients.ClientUtils
 *  org.apache.kafka.clients.GroupRebalanceConfig
 *  org.apache.kafka.clients.GroupRebalanceConfig$ProtocolType
 *  org.apache.kafka.clients.KafkaClient
 *  org.apache.kafka.clients.Metadata
 *  org.apache.kafka.clients.Metadata$LeaderAndEpoch
 *  org.apache.kafka.clients.NetworkClient
 *  org.apache.kafka.clients.consumer.Consumer
 *  org.apache.kafka.clients.consumer.ConsumerConfig
 *  org.apache.kafka.clients.consumer.ConsumerGroupMetadata
 *  org.apache.kafka.clients.consumer.ConsumerInterceptor
 *  org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
 *  org.apache.kafka.clients.consumer.ConsumerRebalanceListener
 *  org.apache.kafka.clients.consumer.ConsumerRecords
 *  org.apache.kafka.clients.consumer.OffsetAndMetadata
 *  org.apache.kafka.clients.consumer.OffsetAndTimestamp
 *  org.apache.kafka.clients.consumer.OffsetCommitCallback
 *  org.apache.kafka.clients.consumer.OffsetResetStrategy
 *  org.apache.kafka.clients.consumer.internals.ConsumerCoordinator
 *  org.apache.kafka.clients.consumer.internals.ConsumerInterceptors
 *  org.apache.kafka.clients.consumer.internals.ConsumerMetadata
 *  org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient
 *  org.apache.kafka.clients.consumer.internals.Fetch
 *  org.apache.kafka.clients.consumer.internals.Fetcher
 *  org.apache.kafka.clients.consumer.internals.FetcherMetricsRegistry
 *  org.apache.kafka.clients.consumer.internals.KafkaConsumerMetrics
 *  org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
 *  org.apache.kafka.clients.consumer.internals.SubscriptionState
 *  org.apache.kafka.clients.consumer.internals.SubscriptionState$FetchPosition
 *  org.apache.kafka.common.Cluster
 *  org.apache.kafka.common.IsolationLevel
 *  org.apache.kafka.common.KafkaException
 *  org.apache.kafka.common.Metric
 *  org.apache.kafka.common.MetricName
 *  org.apache.kafka.common.PartitionInfo
 *  org.apache.kafka.common.TopicPartition
 *  org.apache.kafka.common.config.AbstractConfig
 *  org.apache.kafka.common.errors.InterruptException
 *  org.apache.kafka.common.errors.InvalidGroupIdException
 *  org.apache.kafka.common.errors.TimeoutException
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
 *  org.apache.kafka.common.requests.MetadataRequest$Builder
 *  org.apache.kafka.common.serialization.Deserializer
 *  org.apache.kafka.common.utils.AppInfoParser
 *  org.apache.kafka.common.utils.LogContext
 *  org.apache.kafka.common.utils.Time
 *  org.apache.kafka.common.utils.Timer
 *  org.apache.kafka.common.utils.Utils
 *  org.slf4j.Logger
 */
package org.apache.kafka.clients.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.Fetch;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.FetcherMetricsRegistry;
import org.apache.kafka.clients.consumer.internals.KafkaConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
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
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

public class KafkaConsumer<K, V>
implements Consumer<K, V> {
    private static final String CLIENT_ID_METRIC_TAG = "client-id";
    private static final long NO_CURRENT_THREAD = -1L;
    private static final String JMX_PREFIX = "kafka.consumer";
    static final long DEFAULT_CLOSE_TIMEOUT_MS = 30000L;
    static final String DEFAULT_REASON = "rebalance enforced by user";
    final Metrics metrics;
    final KafkaConsumerMetrics kafkaConsumerMetrics;
    private Logger log;
    private final String clientId;
    private final Optional<String> groupId;
    private final ConsumerCoordinator coordinator;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final Fetcher<K, V> fetcher;
    private final ConsumerInterceptors<K, V> interceptors;
    private final IsolationLevel isolationLevel;
    private final Time time;
    private final ConsumerNetworkClient client;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final long retryBackoffMs;
    private final long requestTimeoutMs;
    private final int defaultApiTimeoutMs;
    private volatile boolean closed = false;
    private List<ConsumerPartitionAssignor> assignors;
    private final AtomicLong currentThread = new AtomicLong(-1L);
    private final AtomicInteger refcount = new AtomicInteger(0);
    private boolean cachedSubscriptionHasAllFetchPositions;

    public KafkaConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    public KafkaConsumer(Properties properties) {
        this(properties, null, null);
    }

    public KafkaConsumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this((Map<String, Object>)Utils.propsToMap((Properties)properties), keyDeserializer, valueDeserializer);
    }

    public KafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)), keyDeserializer, valueDeserializer);
    }

    KafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        try {
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig((AbstractConfig)config, GroupRebalanceConfig.ProtocolType.CONSUMER);
            this.groupId = Optional.ofNullable((Object)groupRebalanceConfig.groupId);
            this.clientId = config.getString("client.id");
            LogContext logContext = groupRebalanceConfig.groupInstanceId.isPresent() ? new LogContext("[Consumer instanceId=" + (String)groupRebalanceConfig.groupInstanceId.get() + ", clientId=" + this.clientId + ", groupId=" + (String)this.groupId.orElse((Object)"null") + "] ") : new LogContext("[Consumer clientId=" + this.clientId + ", groupId=" + (String)this.groupId.orElse((Object)"null") + "] ");
            this.log = logContext.logger(this.getClass());
            boolean enableAutoCommit = config.maybeOverrideEnableAutoCommit();
            this.groupId.ifPresent(groupIdStr -> {
                if (groupIdStr.isEmpty()) {
                    this.log.warn("Support for using the empty group id by consumers is deprecated and will be removed in the next major release.");
                }
            });
            this.log.debug("Initializing the Kafka consumer");
            this.requestTimeoutMs = config.getInt("request.timeout.ms").intValue();
            this.defaultApiTimeoutMs = config.getInt("default.api.timeout.ms");
            this.time = Time.SYSTEM;
            this.metrics = KafkaConsumer.buildMetrics(config, this.time, this.clientId);
            this.retryBackoffMs = config.getLong("retry.backoff.ms");
            List interceptorList = config.getConfiguredInstances("interceptor.classes", ConsumerInterceptor.class, Collections.singletonMap((Object)"client.id", (Object)this.clientId));
            this.interceptors = new ConsumerInterceptors(interceptorList);
            if (keyDeserializer == null) {
                this.keyDeserializer = (Deserializer)config.getConfiguredInstance("key.deserializer", Deserializer.class);
                this.keyDeserializer.configure(config.originals(Collections.singletonMap((Object)"client.id", (Object)this.clientId)), true);
            } else {
                config.ignore("key.deserializer");
                this.keyDeserializer = keyDeserializer;
            }
            if (valueDeserializer == null) {
                this.valueDeserializer = (Deserializer)config.getConfiguredInstance("value.deserializer", Deserializer.class);
                this.valueDeserializer.configure(config.originals(Collections.singletonMap((Object)"client.id", (Object)this.clientId)), false);
            } else {
                config.ignore("value.deserializer");
                this.valueDeserializer = valueDeserializer;
            }
            OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.valueOf((String)config.getString("auto.offset.reset").toUpperCase(Locale.ROOT));
            this.subscriptions = new SubscriptionState(logContext, offsetResetStrategy);
            ClusterResourceListeners clusterResourceListeners = this.configureClusterResourceListeners(keyDeserializer, valueDeserializer, this.metrics.reporters(), interceptorList);
            this.metadata = new ConsumerMetadata(this.retryBackoffMs, config.getLong("metadata.max.age.ms").longValue(), config.getBoolean("exclude.internal.topics") == false, config.getBoolean("allow.auto.create.topics").booleanValue(), this.subscriptions, logContext, clusterResourceListeners);
            List addresses = ClientUtils.parseAndValidateAddresses((List)config.getList("bootstrap.servers"), (String)config.getString("client.dns.lookup"));
            this.metadata.bootstrap(addresses);
            String metricGrpPrefix = "consumer";
            FetcherMetricsRegistry metricsRegistry = new FetcherMetricsRegistry(Collections.singleton((Object)CLIENT_ID_METRIC_TAG), metricGrpPrefix);
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder((AbstractConfig)config, (Time)this.time, (LogContext)logContext);
            this.isolationLevel = IsolationLevel.valueOf((String)config.getString("isolation.level").toUpperCase(Locale.ROOT));
            Sensor throttleTimeSensor = Fetcher.throttleTimeSensor((Metrics)this.metrics, (FetcherMetricsRegistry)metricsRegistry);
            int heartbeatIntervalMs = config.getInt("heartbeat.interval.ms");
            ApiVersions apiVersions = new ApiVersions();
            NetworkClient netClient = new NetworkClient((Selectable)new Selector(config.getLong("connections.max.idle.ms").longValue(), this.metrics, this.time, metricGrpPrefix, channelBuilder, logContext), (Metadata)this.metadata, this.clientId, 100, config.getLong("reconnect.backoff.ms").longValue(), config.getLong("reconnect.backoff.max.ms").longValue(), config.getInt("send.buffer.bytes").intValue(), config.getInt("receive.buffer.bytes").intValue(), config.getInt("request.timeout.ms").intValue(), config.getLong("socket.connection.setup.timeout.ms").longValue(), config.getLong("socket.connection.setup.timeout.max.ms").longValue(), this.time, true, apiVersions, throttleTimeSensor, logContext);
            this.client = new ConsumerNetworkClient(logContext, (KafkaClient)netClient, (Metadata)this.metadata, this.time, this.retryBackoffMs, config.getInt("request.timeout.ms").intValue(), heartbeatIntervalMs);
            this.assignors = ConsumerPartitionAssignor.getAssignorInstances((List)config.getList("partition.assignment.strategy"), (Map)config.originals(Collections.singletonMap((Object)"client.id", (Object)this.clientId)));
            this.coordinator = !this.groupId.isPresent() ? null : new ConsumerCoordinator(groupRebalanceConfig, logContext, this.client, this.assignors, this.metadata, this.subscriptions, this.metrics, metricGrpPrefix, this.time, enableAutoCommit, config.getInt("auto.commit.interval.ms").intValue(), this.interceptors, config.getBoolean("internal.throw.on.fetch.stable.offset.unsupported").booleanValue());
            this.fetcher = new Fetcher(logContext, this.client, config.getInt("fetch.min.bytes").intValue(), config.getInt("fetch.max.bytes").intValue(), config.getInt("fetch.max.wait.ms").intValue(), config.getInt("max.partition.fetch.bytes").intValue(), config.getInt("max.poll.records").intValue(), config.getBoolean("check.crcs").booleanValue(), config.getString("client.rack"), this.keyDeserializer, this.valueDeserializer, this.metadata, this.subscriptions, this.metrics, metricsRegistry, this.time, this.retryBackoffMs, this.requestTimeoutMs, this.isolationLevel, apiVersions);
            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(this.metrics, metricGrpPrefix);
            config.logUnused();
            AppInfoParser.registerAppInfo((String)JMX_PREFIX, (String)this.clientId, (Metrics)this.metrics, (long)this.time.milliseconds());
            this.log.debug("Kafka consumer initialized");
        }
        catch (Throwable t) {
            if (this.log != null) {
                this.close(0L, true);
            }
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    KafkaConsumer(LogContext logContext, String clientId, ConsumerCoordinator coordinator, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Fetcher<K, V> fetcher, ConsumerInterceptors<K, V> interceptors, Time time, ConsumerNetworkClient client, Metrics metrics, SubscriptionState subscriptions, ConsumerMetadata metadata, long retryBackoffMs, long requestTimeoutMs, int defaultApiTimeoutMs, List<ConsumerPartitionAssignor> assignors, String groupId) {
        this.log = logContext.logger(this.getClass());
        this.clientId = clientId;
        this.coordinator = coordinator;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.fetcher = fetcher;
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        this.interceptors = (ConsumerInterceptors)Objects.requireNonNull(interceptors);
        this.time = time;
        this.client = client;
        this.metrics = metrics;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.assignors = assignors;
        this.groupId = Optional.ofNullable((Object)groupId);
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, "consumer");
    }

    private static Metrics buildMetrics(ConsumerConfig config, Time time, String clientId) {
        Map metricsTags = Collections.singletonMap((Object)CLIENT_ID_METRIC_TAG, (Object)clientId);
        MetricConfig metricConfig = new MetricConfig().samples(config.getInt("metrics.num.samples").intValue()).timeWindow(config.getLong("metrics.sample.window.ms").longValue(), TimeUnit.MILLISECONDS).recordLevel(Sensor.RecordingLevel.forName((String)config.getString("metrics.recording.level"))).tags(metricsTags);
        List reporters = config.getConfiguredInstances("metric.reporters", MetricsReporter.class, Collections.singletonMap((Object)"client.id", (Object)clientId));
        JmxReporter jmxReporter = new JmxReporter();
        jmxReporter.configure(config.originals(Collections.singletonMap((Object)"client.id", (Object)clientId)));
        reporters.add((Object)jmxReporter);
        KafkaMetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX, config.originalsWithPrefix("metrics.context."));
        return new Metrics(metricConfig, reporters, time, (MetricsContext)metricsContext);
    }

    public Set<TopicPartition> assignment() {
        this.acquireAndEnsureOpen();
        try {
            Set set = Collections.unmodifiableSet((Set)this.subscriptions.assignedPartitions());
            return set;
        }
        finally {
            this.release();
        }
    }

    public Set<String> subscription() {
        this.acquireAndEnsureOpen();
        try {
            Set set = Collections.unmodifiableSet((Set)new HashSet((Collection)this.subscriptions.subscription()));
            return set;
        }
        finally {
            this.release();
        }
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        this.acquireAndEnsureOpen();
        try {
            this.maybeThrowInvalidGroupIdException();
            if (topics == null) {
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            }
            if (topics.isEmpty()) {
                this.unsubscribe();
            } else {
                for (String topic : topics) {
                    if (!Utils.isBlank((String)topic)) continue;
                    throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                }
                this.throwIfNoAssignorsConfigured();
                this.fetcher.clearBufferedDataForUnassignedTopics(topics);
                this.log.info("Subscribed to topic(s): {}", (Object)Utils.join(topics, (String)", "));
                if (this.subscriptions.subscribe((Set)new HashSet(topics), listener)) {
                    this.metadata.requestUpdateForNewTopics();
                }
            }
        }
        finally {
            this.release();
        }
    }

    public void subscribe(Collection<String> topics) {
        this.subscribe(topics, (ConsumerRebalanceListener)new NoOpConsumerRebalanceListener());
    }

    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        this.maybeThrowInvalidGroupIdException();
        if (pattern == null || pattern.toString().equals((Object)"")) {
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ? "null" : "empty"));
        }
        this.acquireAndEnsureOpen();
        try {
            this.throwIfNoAssignorsConfigured();
            this.log.info("Subscribed to pattern: '{}'", (Object)pattern);
            this.subscriptions.subscribe(pattern, listener);
            this.coordinator.updatePatternSubscription(this.metadata.fetch());
            this.metadata.requestUpdateForNewTopics();
        }
        finally {
            this.release();
        }
    }

    public void subscribe(Pattern pattern) {
        this.subscribe(pattern, (ConsumerRebalanceListener)new NoOpConsumerRebalanceListener());
    }

    public void unsubscribe() {
        this.acquireAndEnsureOpen();
        try {
            this.fetcher.clearBufferedDataForUnassignedPartitions((Collection)Collections.emptySet());
            if (this.coordinator != null) {
                this.coordinator.onLeavePrepare();
                this.coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics");
            }
            this.subscriptions.unsubscribe();
            this.log.info("Unsubscribed all topics or patterns and assigned partitions");
        }
        finally {
            this.release();
        }
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public void assign(Collection<TopicPartition> partitions) {
        this.acquireAndEnsureOpen();
        try {
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
            }
            if (partitions.isEmpty()) {
                this.unsubscribe();
            } else {
                for (TopicPartition tp : partitions) {
                    String topic = tp != null ? tp.topic() : null;
                    if (!Utils.isBlank((String)topic)) continue;
                    throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
                }
                this.fetcher.clearBufferedDataForUnassignedPartitions(partitions);
                if (this.coordinator != null) {
                    this.coordinator.maybeAutoCommitOffsetsAsync(this.time.milliseconds());
                }
                this.log.info("Subscribed to partition(s): {}", (Object)Utils.join(partitions, (String)", "));
                if (this.subscriptions.assignFromUser((Set)new HashSet(partitions))) {
                    this.metadata.requestUpdateForNewTopics();
                }
            }
        }
        finally {
            this.release();
        }
    }

    @Deprecated
    public ConsumerRecords<K, V> poll(long timeoutMs) {
        return this.poll(this.time.timer(timeoutMs), false);
    }

    public ConsumerRecords<K, V> poll(Duration timeout) {
        return this.poll(this.time.timer(timeout), true);
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    private ConsumerRecords<K, V> poll(Timer timer, boolean includeMetadataInTimeout) {
        this.acquireAndEnsureOpen();
        try {
            this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());
            if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }
            do {
                this.client.maybeTriggerWakeup();
                if (includeMetadataInTimeout) {
                    this.updateAssignmentMetadataIfNeeded(timer, false);
                } else {
                    while (!this.updateAssignmentMetadataIfNeeded(this.time.timer(Long.MAX_VALUE), true)) {
                        this.log.warn("Still waiting for metadata");
                    }
                }
                Fetch<K, V> fetch = this.pollForFetches(timer);
                if (fetch.isEmpty()) continue;
                if (this.fetcher.sendFetches() > 0 || this.client.hasPendingRequests()) {
                    this.client.transmitSends();
                }
                if (fetch.records().isEmpty()) {
                    this.log.trace("Returning empty records from `poll()` since the consumer's position has advanced for at least one topic partition");
                }
                ConsumerRecords consumerRecords = this.interceptors.onConsume(new ConsumerRecords(fetch.records()));
                return consumerRecords;
            } while (timer.notExpired());
            ConsumerRecords consumerRecords = ConsumerRecords.empty();
            return consumerRecords;
        }
        finally {
            this.release();
            this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
        }
    }

    boolean updateAssignmentMetadataIfNeeded(Timer timer, boolean waitForJoinGroup) {
        if (this.coordinator != null && !this.coordinator.poll(timer, waitForJoinGroup)) {
            return false;
        }
        return this.updateFetchPositions(timer);
    }

    private Fetch<K, V> pollForFetches(Timer timer) {
        long pollTimeout = this.coordinator == null ? timer.remainingMs() : Math.min((long)this.coordinator.timeToNextPoll(timer.currentTimeMs()), (long)timer.remainingMs());
        Fetch fetch = this.fetcher.collectFetch();
        if (!fetch.isEmpty()) {
            return fetch;
        }
        this.fetcher.sendFetches();
        if (!this.cachedSubscriptionHasAllFetchPositions && pollTimeout > this.retryBackoffMs) {
            pollTimeout = this.retryBackoffMs;
        }
        this.log.trace("Polling for fetches with timeout {}", (Object)pollTimeout);
        Timer pollTimer = this.time.timer(pollTimeout);
        this.client.poll(pollTimer, () -> !this.fetcher.hasAvailableFetches());
        timer.update(pollTimer.currentTimeMs());
        return this.fetcher.collectFetch();
    }

    public void commitSync() {
        this.commitSync(Duration.ofMillis((long)this.defaultApiTimeoutMs));
    }

    public void commitSync(Duration timeout) {
        this.commitSync((Map<TopicPartition, OffsetAndMetadata>)this.subscriptions.allConsumed(), timeout);
    }

    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.commitSync(offsets, Duration.ofMillis((long)this.defaultApiTimeoutMs));
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        this.acquireAndEnsureOpen();
        long commitStart = this.time.nanoseconds();
        try {
            this.maybeThrowInvalidGroupIdException();
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            if (!this.coordinator.commitOffsetsSync((Map)new HashMap(offsets), this.time.timer(timeout))) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before successfully committing offsets " + offsets);
            }
        }
        finally {
            this.kafkaConsumerMetrics.recordCommitSync(this.time.nanoseconds() - commitStart);
            this.release();
        }
    }

    public void commitAsync() {
        this.commitAsync(null);
    }

    public void commitAsync(OffsetCommitCallback callback) {
        this.commitAsync((Map<TopicPartition, OffsetAndMetadata>)this.subscriptions.allConsumed(), callback);
    }

    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        this.acquireAndEnsureOpen();
        try {
            this.maybeThrowInvalidGroupIdException();
            this.log.debug("Committing offsets: {}", offsets);
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            this.coordinator.commitOffsetsAsync((Map)new HashMap(offsets), callback);
        }
        finally {
            this.release();
        }
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public void seek(TopicPartition partition, long offset) {
        if (offset < 0L) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }
        this.acquireAndEnsureOpen();
        try {
            this.log.info("Seeking to offset {} for partition {}", (Object)offset, (Object)partition);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(offset, Optional.empty(), this.metadata.currentLeader(partition));
            this.subscriptions.seekUnvalidated(partition, newPosition);
        }
        finally {
            this.release();
        }
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        long offset = offsetAndMetadata.offset();
        if (offset < 0L) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }
        this.acquireAndEnsureOpen();
        try {
            if (offsetAndMetadata.leaderEpoch().isPresent()) {
                this.log.info("Seeking to offset {} for partition {} with epoch {}", new Object[]{offset, partition, offsetAndMetadata.leaderEpoch().get()});
            } else {
                this.log.info("Seeking to offset {} for partition {}", (Object)offset, (Object)partition);
            }
            Metadata.LeaderAndEpoch currentLeaderAndEpoch = this.metadata.currentLeader(partition);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(), currentLeaderAndEpoch);
            this.updateLastSeenEpochIfNewer(partition, offsetAndMetadata);
            this.subscriptions.seekUnvalidated(partition, newPosition);
        }
        finally {
            this.release();
        }
    }

    public void seekToBeginning(Collection<TopicPartition> partitions) {
        if (partitions == null) {
            throw new IllegalArgumentException("Partitions collection cannot be null");
        }
        this.acquireAndEnsureOpen();
        try {
            Set parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            this.subscriptions.requestOffsetReset((Collection)parts, OffsetResetStrategy.EARLIEST);
        }
        finally {
            this.release();
        }
    }

    public void seekToEnd(Collection<TopicPartition> partitions) {
        if (partitions == null) {
            throw new IllegalArgumentException("Partitions collection cannot be null");
        }
        this.acquireAndEnsureOpen();
        try {
            Set parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            this.subscriptions.requestOffsetReset((Collection)parts, OffsetResetStrategy.LATEST);
        }
        finally {
            this.release();
        }
    }

    public long position(TopicPartition partition) {
        return this.position(partition, Duration.ofMillis((long)this.defaultApiTimeoutMs));
    }

    public long position(TopicPartition partition, Duration timeout) {
        this.acquireAndEnsureOpen();
        try {
            if (!this.subscriptions.isAssigned(partition)) {
                throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");
            }
            Timer timer = this.time.timer(timeout);
            do {
                SubscriptionState.FetchPosition position;
                if ((position = this.subscriptions.validPosition(partition)) != null) {
                    long l = position.offset;
                    return l;
                }
                this.updateFetchPositions(timer);
                this.client.poll(timer);
            } while (timer.notExpired());
            throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position for partition " + partition + " could be determined");
        }
        finally {
            this.release();
        }
    }

    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition) {
        return this.committed(partition, Duration.ofMillis((long)this.defaultApiTimeoutMs));
    }

    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return (OffsetAndMetadata)this.committed((Set<TopicPartition>)Collections.singleton((Object)partition), timeout).get((Object)partition);
    }

    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return this.committed(partitions, Duration.ofMillis((long)this.defaultApiTimeoutMs));
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        this.acquireAndEnsureOpen();
        long start = this.time.nanoseconds();
        try {
            this.maybeThrowInvalidGroupIdException();
            Map offsets = this.coordinator.fetchCommittedOffsets(partitions, this.time.timer(timeout));
            if (offsets == null) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last committed offset for partitions " + partitions + " could be determined. Try tuning default.api.timeout.ms larger to relax the threshold.");
            }
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            Map map = offsets;
            return map;
        }
        finally {
            this.kafkaConsumerMetrics.recordCommitted(this.time.nanoseconds() - start);
            this.release();
        }
    }

    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap((Map)this.metrics.metrics());
    }

    public List<PartitionInfo> partitionsFor(String topic) {
        return this.partitionsFor(topic, Duration.ofMillis((long)this.defaultApiTimeoutMs));
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        this.acquireAndEnsureOpen();
        try {
            Cluster cluster = this.metadata.fetch();
            List parts = cluster.partitionsForTopic(topic);
            if (!parts.isEmpty()) {
                List list = parts;
                return list;
            }
            Timer timer = this.time.timer(timeout);
            Map topicMetadata = this.fetcher.getTopicMetadata(new MetadataRequest.Builder(Collections.singletonList((Object)topic), this.metadata.allowAutoTopicCreation()), timer);
            List list = (List)topicMetadata.getOrDefault((Object)topic, (Object)Collections.emptyList());
            return list;
        }
        finally {
            this.release();
        }
    }

    public Map<String, List<PartitionInfo>> listTopics() {
        return this.listTopics(Duration.ofMillis((long)this.defaultApiTimeoutMs));
    }

    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        this.acquireAndEnsureOpen();
        try {
            Map map = this.fetcher.getAllTopicMetadata(this.time.timer(timeout));
            return map;
        }
        finally {
            this.release();
        }
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public void pause(Collection<TopicPartition> partitions) {
        this.acquireAndEnsureOpen();
        try {
            this.log.debug("Pausing partitions {}", partitions);
            for (TopicPartition partition : partitions) {
                this.subscriptions.pause(partition);
            }
        }
        finally {
            this.release();
        }
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public void resume(Collection<TopicPartition> partitions) {
        this.acquireAndEnsureOpen();
        try {
            this.log.debug("Resuming partitions {}", partitions);
            for (TopicPartition partition : partitions) {
                this.subscriptions.resume(partition);
            }
        }
        finally {
            this.release();
        }
    }

    public Set<TopicPartition> paused() {
        this.acquireAndEnsureOpen();
        try {
            Set set = Collections.unmodifiableSet((Set)this.subscriptions.pausedPartitions());
            return set;
        }
        finally {
            this.release();
        }
    }

    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return this.offsetsForTimes(timestampsToSearch, Duration.ofMillis((long)this.defaultApiTimeoutMs));
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        this.acquireAndEnsureOpen();
        try {
            for (Map.Entry entry : timestampsToSearch.entrySet()) {
                if ((Long)entry.getValue() >= 0L) continue;
                throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " + entry.getValue() + ". The target time cannot be negative.");
            }
            Iterator iterator = this.fetcher.offsetsForTimes(timestampsToSearch, this.time.timer(timeout));
            return iterator;
        }
        finally {
            this.release();
        }
    }

    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return this.beginningOffsets(partitions, Duration.ofMillis((long)this.defaultApiTimeoutMs));
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        this.acquireAndEnsureOpen();
        try {
            Map map = this.fetcher.beginningOffsets(partitions, this.time.timer(timeout));
            return map;
        }
        finally {
            this.release();
        }
    }

    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return this.endOffsets(partitions, Duration.ofMillis((long)this.defaultApiTimeoutMs));
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        this.acquireAndEnsureOpen();
        try {
            Map map = this.fetcher.endOffsets(partitions, this.time.timer(timeout));
            return map;
        }
        finally {
            this.release();
        }
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public OptionalLong currentLag(TopicPartition topicPartition) {
        this.acquireAndEnsureOpen();
        try {
            Long lag = this.subscriptions.partitionLag(topicPartition, this.isolationLevel);
            if (lag == null) {
                if (this.subscriptions.partitionEndOffset(topicPartition, this.isolationLevel) == null && !this.subscriptions.partitionEndOffsetRequested(topicPartition)) {
                    this.log.info("Requesting the log end offset for {} in order to compute lag", (Object)topicPartition);
                    this.subscriptions.requestPartitionEndOffset(topicPartition);
                    this.fetcher.endOffsets((Collection)Collections.singleton((Object)topicPartition), this.time.timer(0L));
                }
                OptionalLong optionalLong = OptionalLong.empty();
                return optionalLong;
            }
            OptionalLong optionalLong = OptionalLong.of((long)lag);
            return optionalLong;
        }
        finally {
            this.release();
        }
    }

    public ConsumerGroupMetadata groupMetadata() {
        this.acquireAndEnsureOpen();
        try {
            this.maybeThrowInvalidGroupIdException();
            ConsumerGroupMetadata consumerGroupMetadata = this.coordinator.groupMetadata();
            return consumerGroupMetadata;
        }
        finally {
            this.release();
        }
    }

    public void enforceRebalance(String reason) {
        this.acquireAndEnsureOpen();
        try {
            if (this.coordinator == null) {
                throw new IllegalStateException("Tried to force a rebalance but consumer does not have a group.");
            }
            this.coordinator.requestRejoin(reason == null || reason.isEmpty() ? DEFAULT_REASON : reason);
        }
        finally {
            this.release();
        }
    }

    public void enforceRebalance() {
        this.enforceRebalance(null);
    }

    public void close() {
        this.close(Duration.ofMillis((long)30000L));
    }

    public void close(Duration timeout) {
        if (timeout.toMillis() < 0L) {
            throw new IllegalArgumentException("The timeout cannot be negative.");
        }
        this.acquire();
        try {
            if (!this.closed) {
                this.close(timeout.toMillis(), false);
            }
        }
        finally {
            this.closed = true;
            this.release();
        }
    }

    public void wakeup() {
        this.client.wakeup();
    }

    private ClusterResourceListeners configureClusterResourceListeners(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, List<?> ... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList : candidateLists) {
            clusterResourceListeners.maybeAddAll(candidateList);
        }
        clusterResourceListeners.maybeAdd(keyDeserializer);
        clusterResourceListeners.maybeAdd(valueDeserializer);
        return clusterResourceListeners;
    }

    private void close(long timeoutMs, boolean swallowException) {
        this.log.trace("Closing the Kafka consumer");
        AtomicReference firstException = new AtomicReference();
        try {
            if (this.coordinator != null) {
                this.coordinator.close(this.time.timer(Math.min((long)timeoutMs, (long)this.requestTimeoutMs)));
            }
        }
        catch (Throwable t) {
            firstException.compareAndSet(null, (Object)t);
            this.log.error("Failed to close coordinator", t);
        }
        Utils.closeQuietly(this.fetcher, (String)"fetcher", (AtomicReference)firstException);
        Utils.closeQuietly(this.interceptors, (String)"consumer interceptors", (AtomicReference)firstException);
        Utils.closeQuietly((AutoCloseable)this.kafkaConsumerMetrics, (String)"kafka consumer metrics", (AtomicReference)firstException);
        Utils.closeQuietly((AutoCloseable)this.metrics, (String)"consumer metrics", (AtomicReference)firstException);
        Utils.closeQuietly((AutoCloseable)this.client, (String)"consumer network client", (AtomicReference)firstException);
        Utils.closeQuietly(this.keyDeserializer, (String)"consumer key deserializer", (AtomicReference)firstException);
        Utils.closeQuietly(this.valueDeserializer, (String)"consumer value deserializer", (AtomicReference)firstException);
        AppInfoParser.unregisterAppInfo((String)JMX_PREFIX, (String)this.clientId, (Metrics)this.metrics);
        this.log.debug("Kafka consumer has been closed");
        Throwable exception = (Throwable)((Object)firstException.get());
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException)exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    private boolean updateFetchPositions(Timer timer) {
        this.fetcher.validateOffsetsIfNeeded();
        this.cachedSubscriptionHasAllFetchPositions = this.subscriptions.hasAllFetchPositions();
        if (this.cachedSubscriptionHasAllFetchPositions) {
            return true;
        }
        if (this.coordinator != null && !this.coordinator.refreshCommittedOffsetsIfNeeded(timer)) {
            return false;
        }
        this.subscriptions.resetInitializingPositions();
        this.fetcher.resetOffsetsIfNeeded();
        return true;
    }

    private void acquireAndEnsureOpen() {
        this.acquire();
        if (this.closed) {
            this.release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    private void acquire() {
        long threadId = Thread.currentThread().getId();
        if (threadId != this.currentThread.get() && !this.currentThread.compareAndSet(-1L, threadId)) {
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access");
        }
        this.refcount.incrementAndGet();
    }

    private void release() {
        if (this.refcount.decrementAndGet() == 0) {
            this.currentThread.set(-1L);
        }
    }

    private void throwIfNoAssignorsConfigured() {
        if (this.assignors.isEmpty()) {
            throw new IllegalStateException("Must configure at least one partition assigner class name to partition.assignment.strategy configuration property");
        }
    }

    private void maybeThrowInvalidGroupIdException() {
        if (!this.groupId.isPresent()) {
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must provide a valid group.id in the consumer configuration.");
        }
    }

    private void updateLastSeenEpochIfNewer(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        if (offsetAndMetadata != null) {
            offsetAndMetadata.leaderEpoch().ifPresent(epoch -> this.metadata.updateLastSeenEpochIfNewer(topicPartition, epoch.intValue()));
        }
    }

    String getClientId() {
        return this.clientId;
    }

    boolean updateAssignmentMetadataIfNeeded(Timer timer) {
        return this.updateAssignmentMetadataIfNeeded(timer, true);
    }
}
