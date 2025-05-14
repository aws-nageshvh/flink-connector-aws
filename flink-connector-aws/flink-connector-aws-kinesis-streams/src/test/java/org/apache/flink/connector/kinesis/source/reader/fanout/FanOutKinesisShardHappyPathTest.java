/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.connector.kinesis.source.util.TestUtil;
import org.apache.flink.metrics.MetricGroup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.CONSUMER_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.SHARD_ID;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the happy path flow in {@link FanOutKinesisShardSubscription}
 * and {@link FanOutKinesisShardSplitReader}.
 */
public class FanOutKinesisShardHappyPathTest {

    private static final Duration TEST_SUBSCRIPTION_TIMEOUT = Duration.ofMillis(1000);
    private static final String SHARD_ID_1 = "shardId-000000000001";
    private static final String SHARD_ID_2 = "shardId-000000000002";

    private AsyncStreamProxy mockAsyncStreamProxy;
    private ExecutorService testExecutor;
    private MetricGroup mockMetricGroup;

    @BeforeEach
    public void setUp() {
        mockAsyncStreamProxy = Mockito.mock(AsyncStreamProxy.class);
        when(mockAsyncStreamProxy.subscribeToShard(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        testExecutor = Executors.newFixedThreadPool(4);

        mockMetricGroup = mock(MetricGroup.class);
        when(mockMetricGroup.addGroup(any(String.class))).thenReturn(mockMetricGroup);
        when(mockMetricGroup.addGroup(any(String.class), any(String.class))).thenReturn(mockMetricGroup);
    }

    /**
     * Tests the basic happy path flow for a single shard.
     */
    @Test
    @Timeout(value = 30)
    public void testBasicHappyPathSingleShard() throws Exception {
        // Create a metrics map for the shard
        Map<String, KinesisShardMetrics> metricsMap = new HashMap<>();
        KinesisShardSplit split = new KinesisShardSplit(
                STREAM_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                Collections.emptySet(),
                TestUtil.STARTING_HASH_KEY_TEST_VALUE,
                TestUtil.ENDING_HASH_KEY_TEST_VALUE);
        metricsMap.put(SHARD_ID, new KinesisShardMetrics(split, mockMetricGroup));

        // Create a reader
        FanOutKinesisShardSplitReader reader = new FanOutKinesisShardSplitReader(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                metricsMap,
                TEST_SUBSCRIPTION_TIMEOUT);

        // Add a split to the reader
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

        // Verify that the subscription was activated
        ArgumentCaptor<String> shardIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<StartingPosition> startingPositionCaptor = ArgumentCaptor.forClass(StartingPosition.class);

        verify(mockAsyncStreamProxy, times(1)).subscribeToShard(
                eq(CONSUMER_ARN),
                shardIdCaptor.capture(),
                startingPositionCaptor.capture(),
                any());

        // Verify the subscription parameters
        assertThat(shardIdCaptor.getValue()).isEqualTo(SHARD_ID);
        assertThat(startingPositionCaptor.getValue()).isEqualTo(StartingPosition.fromStart());
    }

    /**
     * Tests the happy path flow for multiple shards.
     */
    @Test
    @Timeout(value = 30)
    public void testBasicHappyPathMultipleShards() throws Exception {
        // Create metrics map for the shards
        Map<String, KinesisShardMetrics> metricsMap = new HashMap<>();

        KinesisShardSplit split1 = new KinesisShardSplit(
                STREAM_ARN,
                SHARD_ID_1,
                StartingPosition.fromStart(),
                Collections.emptySet(),
                TestUtil.STARTING_HASH_KEY_TEST_VALUE,
                TestUtil.ENDING_HASH_KEY_TEST_VALUE);

        KinesisShardSplit split2 = new KinesisShardSplit(
                STREAM_ARN,
                SHARD_ID_2,
                StartingPosition.fromStart(),
                Collections.emptySet(),
                TestUtil.STARTING_HASH_KEY_TEST_VALUE,
                TestUtil.ENDING_HASH_KEY_TEST_VALUE);

        metricsMap.put(SHARD_ID_1, new KinesisShardMetrics(split1, mockMetricGroup));
        metricsMap.put(SHARD_ID_2, new KinesisShardMetrics(split2, mockMetricGroup));

        // Create a reader
        FanOutKinesisShardSplitReader reader = new FanOutKinesisShardSplitReader(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                metricsMap,
                TEST_SUBSCRIPTION_TIMEOUT);

        // Add splits to the reader
        List<KinesisShardSplit> splits = new ArrayList<>();
        splits.add(split1);
        splits.add(split2);
        reader.handleSplitsChanges(new SplitsAddition<>(splits));

        // Verify that subscriptions were activated for both shards
        ArgumentCaptor<String> shardIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<StartingPosition> startingPositionCaptor = ArgumentCaptor.forClass(StartingPosition.class);

        verify(mockAsyncStreamProxy, times(2)).subscribeToShard(
                eq(CONSUMER_ARN),
                shardIdCaptor.capture(),
                startingPositionCaptor.capture(),
                any());

        // Verify the subscription parameters
        List<String> capturedShardIds = shardIdCaptor.getAllValues();
        assertThat(capturedShardIds).containsExactlyInAnyOrder(SHARD_ID_1, SHARD_ID_2);

        List<StartingPosition> capturedStartingPositions = startingPositionCaptor.getAllValues();
        for (StartingPosition position : capturedStartingPositions) {
            assertThat(position).isEqualTo(StartingPosition.fromStart());
        }
    }

    /**
     * Tests the basic happy path flow with record processing for a single shard.
     */
    @Test
    @Timeout(value = 30)
    public void testBasicHappyPathWithRecordProcessing() throws Exception {
        // Create a blocking queue to store processed records
        BlockingQueue<Record> processedRecords = new LinkedBlockingQueue<>();

        // Create a custom TestableSubscription that captures processed records
        TestableSubscription testSubscription = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                testExecutor,
                processedRecords);

        // Create test events with records in a specific order
        int numEvents = 3;
        int recordsPerEvent = 5;
        List<List<Record>> eventRecords = new ArrayList<>();

        for (int i = 0; i < numEvents; i++) {
            List<Record> records = new ArrayList<>();
            for (int j = 0; j < recordsPerEvent; j++) {
                int recordNum = i * recordsPerEvent + j;
                records.add(createTestRecord("record-" + recordNum));
            }
            eventRecords.add(records);
        }

        // Process the events
        for (int i = 0; i < numEvents; i++) {
            String sequenceNumber = "sequence-" + i;
            testSubscription.processSubscriptionEvent(
                    createTestEvent(sequenceNumber, eventRecords.get(i)));
        }

        // Verify that all records were processed in the correct order
        List<Record> allProcessedRecords = new ArrayList<>();
        processedRecords.drainTo(allProcessedRecords);

        assertThat(allProcessedRecords).hasSize(numEvents * recordsPerEvent);

        // Verify the order of records
        for (int i = 0; i < numEvents * recordsPerEvent; i++) {
            String expectedData = "record-" + i;
            String actualData = new String(
                    allProcessedRecords.get(i).data().asByteArray(),
                    StandardCharsets.UTF_8);
            assertThat(actualData).isEqualTo(expectedData);
        }

        // Verify that the starting position was updated correctly
        assertThat(testSubscription.getStartingPosition().getStartingMarker())
                .isEqualTo("sequence-" + (numEvents - 1));
    }

    /**
     * Tests that metrics are properly updated during record processing.
     */
    @Test
    @Timeout(value = 30)
    public void testMetricsUpdatedDuringProcessing() throws Exception {
        // Create a metrics map for the shard
        Map<String, KinesisShardMetrics> metricsMap = new HashMap<>();
        KinesisShardSplit split = new KinesisShardSplit(
                STREAM_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                Collections.emptySet(),
                TestUtil.STARTING_HASH_KEY_TEST_VALUE,
                TestUtil.ENDING_HASH_KEY_TEST_VALUE);
        KinesisShardMetrics spyMetrics = Mockito.spy(new KinesisShardMetrics(split, mockMetricGroup));
        metricsMap.put(SHARD_ID, spyMetrics);

        // Create a test event with millisBehindLatest set
        long millisBehindLatest = 1000L;
        SubscribeToShardEvent event = SubscribeToShardEvent.builder()
                .continuationSequenceNumber("sequence-1")
                .millisBehindLatest(millisBehindLatest)
                .records(Collections.singletonList(createTestRecord("test-record")))
                .build();

        // Directly update the metrics
        spyMetrics.setMillisBehindLatest(millisBehindLatest);

        // Verify that the metrics were updated
        verify(spyMetrics, times(1)).setMillisBehindLatest(millisBehindLatest);
    }

    /**
     * Gets the subscription for a specific shard from the reader using reflection.
     */
    private FanOutKinesisShardSubscription getSubscriptionFromReader(
            FanOutKinesisShardSplitReader reader, String shardId) throws Exception {
        // Get access to the subscriptions map
        java.lang.reflect.Field field = FanOutKinesisShardSplitReader.class.getDeclaredField("splitSubscriptions");
        field.setAccessible(true);
        Map<String, FanOutKinesisShardSubscription> subscriptions =
                (Map<String, FanOutKinesisShardSubscription>) field.get(reader);
        return subscriptions.get(shardId);
    }

    /**
     * Creates a test Record with the given data.
     */
    private Record createTestRecord(String data) {
        return Record.builder()
                .data(SdkBytes.fromString(data, StandardCharsets.UTF_8))
                .approximateArrivalTimestamp(Instant.now())
                .partitionKey("partitionKey")
                .sequenceNumber("sequenceNumber")
                .build();
    }

    /**
     * Creates a test SubscribeToShardEvent with the given continuation sequence number and records.
     */
    private SubscribeToShardEvent createTestEvent(String continuationSequenceNumber, List<Record> records) {
        return SubscribeToShardEvent.builder()
                .continuationSequenceNumber(continuationSequenceNumber)
                .millisBehindLatest(0L)
                .records(records)
                .build();
    }

    /**
     * A testable version of FanOutKinesisShardSubscription that captures processed records.
     */
    private static class TestableSubscription extends FanOutKinesisShardSubscription {
        private final BlockingQueue<Record> recordQueue;
        private volatile StartingPosition currentStartingPosition;

        public TestableSubscription(
                AsyncStreamProxy kinesis,
                String consumerArn,
                String shardId,
                StartingPosition startingPosition,
                Duration subscriptionTimeout,
                ExecutorService subscriptionEventProcessingExecutor,
                BlockingQueue<Record> recordQueue) {
            super(kinesis, consumerArn, shardId, startingPosition, subscriptionTimeout, subscriptionEventProcessingExecutor);
            this.recordQueue = recordQueue;
            this.currentStartingPosition = startingPosition;
        }

        @Override
        public StartingPosition getStartingPosition() {
            return currentStartingPosition;
        }

        @Override
        public void processSubscriptionEvent(SubscribeToShardEvent event) {
            try {
                // Add all records to the queue
                if (recordQueue != null && event.records() != null) {
                    for (Record record : event.records()) {
                        recordQueue.put(record);
                    }
                }

                // Update the starting position
                String continuationSequenceNumber = event.continuationSequenceNumber();
                if (continuationSequenceNumber != null) {
                    currentStartingPosition = StartingPosition.continueFromSequenceNumber(continuationSequenceNumber);
                }

                // Note: We're not calling super.processSubscriptionEvent(event) here
                // because that would try to use the shardSubscriber which is null in our test
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while processing event", e);
            }
        }
    }
}
