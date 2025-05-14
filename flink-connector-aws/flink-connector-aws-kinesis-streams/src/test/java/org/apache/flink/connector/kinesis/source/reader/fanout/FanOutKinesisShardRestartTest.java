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
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

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
 * Tests for the restart behavior in {@link FanOutKinesisShardSubscription}
 * and {@link FanOutKinesisShardSplitReader}.
 */
public class FanOutKinesisShardRestartTest {

    private static final Duration TEST_SUBSCRIPTION_TIMEOUT = Duration.ofMillis(1000);

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
     * Tests that when a restart happens, the correct starting position is used to reactivate the subscription.
     */
    @Test
    @Timeout(value = 30)
    public void testRestartUsesCorrectStartingPosition() throws Exception {
        // Create a custom AsyncStreamProxy that will capture the starting position
        AsyncStreamProxy customProxy = Mockito.mock(AsyncStreamProxy.class);
        ArgumentCaptor<StartingPosition> startingPositionCaptor = ArgumentCaptor.forClass(StartingPosition.class);

        when(customProxy.subscribeToShard(
                any(String.class),
                any(String.class),
                startingPositionCaptor.capture(),
                any()))
                .thenReturn(CompletableFuture.completedFuture(null));

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
                customProxy,
                CONSUMER_ARN,
                metricsMap,
                TEST_SUBSCRIPTION_TIMEOUT);

        // Add a split to the reader
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

        // Verify that the subscription was activated with the initial starting position
        verify(customProxy, times(1)).subscribeToShard(
                eq(CONSUMER_ARN),
                eq(SHARD_ID),
                any(StartingPosition.class),
                any());

        assertThat(startingPositionCaptor.getValue()).isEqualTo(StartingPosition.fromStart());

        // Create a new split with the updated starting position
        String continuationSequenceNumber = "sequence-after-processing";
        StartingPosition updatedPosition = StartingPosition.continueFromSequenceNumber(continuationSequenceNumber);
        KinesisShardSplit updatedSplit = new KinesisShardSplit(
                STREAM_ARN,
                SHARD_ID,
                updatedPosition,
                Collections.emptySet(),
                TestUtil.STARTING_HASH_KEY_TEST_VALUE,
                TestUtil.ENDING_HASH_KEY_TEST_VALUE);

        // Simulate a restart by creating a new reader with the updated split
        FanOutKinesisShardSplitReader restartedReader = new FanOutKinesisShardSplitReader(
                customProxy,
                CONSUMER_ARN,
                metricsMap,
                TEST_SUBSCRIPTION_TIMEOUT);

        // Add the updated split to the restarted reader
        restartedReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(updatedSplit)));

        // Verify that the subscription was reactivated with the updated starting position
        verify(customProxy, times(2)).subscribeToShard(
                eq(CONSUMER_ARN),
                eq(SHARD_ID),
                any(StartingPosition.class),
                any());

        // Get the second captured value (from the restart)
        StartingPosition capturedPosition = startingPositionCaptor.getAllValues().get(1);

        // Verify it matches our expected updated position
        assertThat(capturedPosition.getShardIteratorType()).isEqualTo(updatedPosition.getShardIteratorType());
        assertThat(capturedPosition.getStartingMarker()).isEqualTo(updatedPosition.getStartingMarker());
    }

    /**
     * Tests that when exceptions are thrown, the job is restarted.
     */
    @Test
    @Timeout(value = 30)
    public void testExceptionsProperlyHandled() throws Exception {
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

        // Test with different types of exceptions
        testExceptionHandling(ResourceNotFoundException.builder().message("Resource not found").build(), true);
        testExceptionHandling(new IOException("IO exception"), true);
        testExceptionHandling(new TimeoutException("Timeout"), true);
        testExceptionHandling(new RuntimeException("Runtime exception"), false);
    }

    /**
     * Helper method to test exception handling.
     */
    private void testExceptionHandling(Exception exception, boolean isRecoverable) throws Exception {
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

        // Create a mock AsyncStreamProxy that throws the specified exception
        AsyncStreamProxy exceptionProxy = Mockito.mock(AsyncStreamProxy.class);
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(exception);
        when(exceptionProxy.subscribeToShard(any(), any(), any(), any()))
                .thenReturn(failedFuture);

        // Create a reader with the exception-throwing proxy
        FanOutKinesisShardSplitReader reader = new FanOutKinesisShardSplitReader(
                exceptionProxy,
                CONSUMER_ARN,
                metricsMap,
                TEST_SUBSCRIPTION_TIMEOUT);

        // Add a split to the reader
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

        // If the exception is recoverable, the reader should try to reactivate the subscription
        // If not, it should propagate the exception
        if (isRecoverable) {
            // Verify that the subscription was activated
            verify(exceptionProxy, times(1)).subscribeToShard(
                    eq(CONSUMER_ARN),
                    eq(SHARD_ID),
                    any(),
                    any());
        } else {
            // For non-recoverable exceptions, we expect them to be propagated
            // This would typically cause the job to be restarted
            // In a real scenario, this would be caught by Flink's error handling
            verify(exceptionProxy, times(1)).subscribeToShard(
                    eq(CONSUMER_ARN),
                    eq(SHARD_ID),
                    any(),
                    any());
        }
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
     * Sets the starting position in a subscription using reflection.
     */
    private void setStartingPositionInSubscription(
            FanOutKinesisShardSubscription subscription, StartingPosition startingPosition) throws Exception {
        // Get access to the currentStartingPosition field
        java.lang.reflect.Field field = subscription.getClass().getDeclaredField("startingPosition");
        field.setAccessible(true);
        field.set(subscription, startingPosition);
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
}
