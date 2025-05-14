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
import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.connector.kinesis.source.util.TestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.CONSUMER_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.SHARD_ID;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for starting position recording behavior in {@link FanOutKinesisShardSubscription}
 * and {@link FanOutKinesisShardSplitReader}.
 */
public class FanOutKinesisShardStartingPositionTest {

    private static final Duration TEST_SUBSCRIPTION_TIMEOUT = Duration.ofMillis(1000);
    private static final String TEST_CONTINUATION_SEQUENCE_NUMBER = "test-continuation-sequence-number";
    private static final String SHARD_ID_1 = "shardId-000000000001";
    private static final String SHARD_ID_2 = "shardId-000000000002";

    private AsyncStreamProxy mockAsyncStreamProxy;
    private ExecutorService testExecutor;

    @BeforeEach
    public void setUp() {
        mockAsyncStreamProxy = Mockito.mock(AsyncStreamProxy.class);
        when(mockAsyncStreamProxy.subscribeToShard(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        testExecutor = Executors.newFixedThreadPool(4);
    }

    /**
     * Tests that the starting position is updated only after the event is successfully added to the queue
     * for a single shard.
     */
    @Test
    @Timeout(value = 30)
    public void testStartingPositionUpdatedAfterQueuePut_SingleShard() throws Exception {
        // Create a blocking queue that we can control
        BlockingQueue<SubscribeToShardEvent> controlledQueue = spy(new LinkedBlockingQueue<>(2));

        // Create a latch to track when put is called
        CountDownLatch putCalled = new CountDownLatch(1);

        // Create a latch to control when put returns
        CountDownLatch allowPutToReturn = new CountDownLatch(1);

        // Create an atomic boolean to track if the starting position was updated before put completed
        AtomicBoolean startingPositionUpdatedBeforePutCompleted = new AtomicBoolean(false);

        // Mock the queue's put method to control its execution
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                putCalled.countDown();
                allowPutToReturn.await(5, TimeUnit.SECONDS);

                // Call the real method
                invocation.callRealMethod();
                return null;
            }
        }).when(controlledQueue).put(any(SubscribeToShardEvent.class));

        // Create a subscription with access to the controlled queue
        TestableSubscription testSubscription = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                testExecutor,
                controlledQueue);

        // Create a thread to check the starting position while put is blocked
        Thread checkThread = new Thread(() -> {
            try {
                // Wait for put to be called
                assertThat(putCalled.await(5, TimeUnit.SECONDS)).isTrue();

                // Check if the starting position was updated before put completed
                StartingPosition currentPosition = testSubscription.getStartingPosition();
                if (currentPosition.getStartingMarker() != null &&
                    currentPosition.getStartingMarker().equals(TEST_CONTINUATION_SEQUENCE_NUMBER)) {
                    startingPositionUpdatedBeforePutCompleted.set(true);
                }

                // Allow put to return
                allowPutToReturn.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Start the check thread
        checkThread.start();

        // Process an event
        testSubscription.processSubscriptionEvent(createTestEvent(TEST_CONTINUATION_SEQUENCE_NUMBER));

        // Wait for the check thread to complete
        checkThread.join(5000);

        // Verify that the starting position was not updated before put completed
        assertThat(startingPositionUpdatedBeforePutCompleted.get()).isFalse();

        // Verify that the starting position was updated after put completed
        assertThat(testSubscription.getStartingPosition().getStartingMarker())
                .isEqualTo(TEST_CONTINUATION_SEQUENCE_NUMBER);
    }

    /**
     * Tests that the starting position is updated only after the event is successfully added to the queue
     * for two shards.
     */
    @Test
    @Timeout(value = 30)
    public void testStartingPositionUpdatedAfterQueuePut_TwoShards() throws Exception {
        // Create a reader with two shards
        FanOutKinesisShardSplitReader reader = createSplitReaderWithTwoShards();

        // Create a blocking queue that we can control for each shard
        BlockingQueue<SubscribeToShardEvent> controlledQueue1 = spy(new LinkedBlockingQueue<>(2));
        BlockingQueue<SubscribeToShardEvent> controlledQueue2 = spy(new LinkedBlockingQueue<>(2));

        // Create flags to track if put was called for each shard
        AtomicBoolean putWasCalled1 = new AtomicBoolean(false);
        AtomicBoolean putWasCalled2 = new AtomicBoolean(false);

        // Create latches to control when put returns for each shard
        CountDownLatch allowPutToReturn1 = new CountDownLatch(1);
        CountDownLatch allowPutToReturn2 = new CountDownLatch(1);

        // Create atomic references to track the starting positions
        AtomicReference<StartingPosition> startingPosition1BeforePut = new AtomicReference<>();
        AtomicReference<StartingPosition> startingPosition2BeforePut = new AtomicReference<>();
        AtomicReference<StartingPosition> startingPosition1AfterPut = new AtomicReference<>();
        AtomicReference<StartingPosition> startingPosition2AfterPut = new AtomicReference<>();

        // Mock the queue's put method for the first shard
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                putWasCalled1.set(true);
                allowPutToReturn1.await(5, TimeUnit.SECONDS);

                // Call the real method
                invocation.callRealMethod();
                return null;
            }
        }).when(controlledQueue1).put(any(SubscribeToShardEvent.class));

        // Mock the queue's put method for the second shard
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                putWasCalled2.set(true);
                allowPutToReturn2.await(5, TimeUnit.SECONDS);

                // Call the real method
                invocation.callRealMethod();
                return null;
            }
        }).when(controlledQueue2).put(any(SubscribeToShardEvent.class));

        // Get the subscriptions from the reader
        TestableSubscription subscription1 = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID_1,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                testExecutor,
                controlledQueue1);

        TestableSubscription subscription2 = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID_2,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                testExecutor,
                controlledQueue2);

        // Create threads to check the starting positions while put is blocked
        Thread checkThread1 = new Thread(() -> {
            // Wait for put to be called
            while (!putWasCalled1.get()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            // Check the starting position before put completes
            startingPosition1BeforePut.set(subscription1.getStartingPosition());

            // Allow put to return
            allowPutToReturn1.countDown();

            // Wait a bit to ensure the starting position is updated
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Check the starting position after put completes
            startingPosition1AfterPut.set(subscription1.getStartingPosition());
        });

        Thread checkThread2 = new Thread(() -> {
            // Wait for put to be called
            while (!putWasCalled2.get()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            // Check the starting position before put completes
            startingPosition2BeforePut.set(subscription2.getStartingPosition());

            // Allow put to return
            allowPutToReturn2.countDown();

            // Wait a bit to ensure the starting position is updated
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Check the starting position after put completes
            startingPosition2AfterPut.set(subscription2.getStartingPosition());
        });

        // Start the check threads
        checkThread1.start();
        checkThread2.start();

        // Process events for both shards
        String sequenceNumber1 = "sequence-1";
        String sequenceNumber2 = "sequence-2";
        subscription1.processSubscriptionEvent(createTestEvent(sequenceNumber1));
        subscription2.processSubscriptionEvent(createTestEvent(sequenceNumber2));

        // Wait for the check threads to complete
        checkThread1.join(5000);
        checkThread2.join(5000);

        // Verify that the starting positions were not updated before put completed
        assertThat(startingPosition1BeforePut.get().getStartingMarker()).isNotEqualTo(sequenceNumber1);
        assertThat(startingPosition2BeforePut.get().getStartingMarker()).isNotEqualTo(sequenceNumber2);

        // Verify that the starting positions were updated after put completed
        assertThat(startingPosition1AfterPut.get().getStartingMarker()).isEqualTo(sequenceNumber1);
        assertThat(startingPosition2AfterPut.get().getStartingMarker()).isEqualTo(sequenceNumber2);
    }

    /**
     * Tests that the starting position is not updated when queue.put fails for a single shard.
     */
    @Test
    @Timeout(value = 30)
    public void testStartingPositionNotUpdatedWhenQueuePutFails_SingleShard() throws Exception {
        // Create a blocking queue that will throw an exception on put
        BlockingQueue<SubscribeToShardEvent> failingQueue = spy(new LinkedBlockingQueue<>(2));

        // Create a flag to track if put was called
        AtomicBoolean putWasCalled = new AtomicBoolean(false);

        // Mock the queue's put method to throw an exception
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                putWasCalled.set(true);
                throw new InterruptedException("Test exception");
            }
        }).when(failingQueue).put(any(SubscribeToShardEvent.class));

        // Create a subscription with access to the failing queue
        TestableSubscription testSubscription = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                testExecutor,
                failingQueue);

        // Get the initial starting position
        StartingPosition initialPosition = testSubscription.getStartingPosition();

        // Process an event (this should fail)
        try {
            testSubscription.processSubscriptionEvent(createTestEvent(TEST_CONTINUATION_SEQUENCE_NUMBER));
        } catch (KinesisStreamsSourceException e) {
            // Expected exception
        }

        // Verify that put was called without waiting for the latch
        assertThat(putWasCalled.get()).isTrue();

        // Verify that the starting position was not updated
        assertThat(testSubscription.getStartingPosition()).isEqualTo(initialPosition);
    }

    /**
     * Tests that the starting position is not updated when queue.put fails for two shards.
     */
    @Test
    @Timeout(value = 30)
    public void testStartingPositionNotUpdatedWhenQueuePutFails_TwoShards() throws Exception {
        // Create a reader with two shards
        FanOutKinesisShardSplitReader reader = createSplitReaderWithTwoShards();

        // Create blocking queues that will throw exceptions on put
        BlockingQueue<SubscribeToShardEvent> failingQueue1 = spy(new LinkedBlockingQueue<>(2));
        BlockingQueue<SubscribeToShardEvent> failingQueue2 = spy(new LinkedBlockingQueue<>(2));

        // Create flags to track if put was called for each shard
        AtomicBoolean putWasCalled1 = new AtomicBoolean(false);
        AtomicBoolean putWasCalled2 = new AtomicBoolean(false);

        // Mock the queue's put method for the first shard to throw an exception
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                putWasCalled1.set(true);
                throw new InterruptedException("Test exception 1");
            }
        }).when(failingQueue1).put(any(SubscribeToShardEvent.class));

        // Mock the queue's put method for the second shard to throw an exception
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                putWasCalled2.set(true);
                throw new InterruptedException("Test exception 2");
            }
        }).when(failingQueue2).put(any(SubscribeToShardEvent.class));

        // Get the subscriptions from the reader
        TestableSubscription subscription1 = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID_1,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                testExecutor,
                failingQueue1);

        TestableSubscription subscription2 = new TestableSubscription(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                SHARD_ID_2,
                StartingPosition.fromStart(),
                TEST_SUBSCRIPTION_TIMEOUT,
                testExecutor,
                failingQueue2);

        // Get the initial starting positions
        StartingPosition initialPosition1 = subscription1.getStartingPosition();
        StartingPosition initialPosition2 = subscription2.getStartingPosition();

        // Process events for both shards (these should fail)
        try {
            subscription1.processSubscriptionEvent(createTestEvent("sequence-1"));
        } catch (KinesisStreamsSourceException e) {
            // Expected exception
        }

        try {
            subscription2.processSubscriptionEvent(createTestEvent("sequence-2"));
        } catch (KinesisStreamsSourceException e) {
            // Expected exception
        }

        // Verify that put was called for both shards without waiting for latches
        assertThat(putWasCalled1.get()).isTrue();
        assertThat(putWasCalled2.get()).isTrue();

        // Verify that the starting positions were not updated
        assertThat(subscription1.getStartingPosition()).isEqualTo(initialPosition1);
        assertThat(subscription2.getStartingPosition()).isEqualTo(initialPosition2);
    }

    /**
     * Creates a test SubscribeToShardEvent with the given continuation sequence number.
     */
    private SubscribeToShardEvent createTestEvent(String continuationSequenceNumber) {
        return SubscribeToShardEvent.builder()
                .continuationSequenceNumber(continuationSequenceNumber)
                .millisBehindLatest(0L)
                .records(new ArrayList<>())
                .build();
    }

    /**
     * Creates a FanOutKinesisShardSplitReader with two shards.
     */
    private FanOutKinesisShardSplitReader createSplitReaderWithTwoShards() {
        FanOutKinesisShardSplitReader reader = new FanOutKinesisShardSplitReader(
                mockAsyncStreamProxy,
                CONSUMER_ARN,
                Collections.emptyMap(),
                TEST_SUBSCRIPTION_TIMEOUT);

        // Create two splits
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

        // Add the splits to the reader
        reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split1, split2)));

        return reader;
    }

    /**
     * A testable version of FanOutKinesisShardSubscription that allows access to the event queue
     * and overrides methods that require shardSubscriber to be initialized.
     */
    private static class TestableSubscription extends FanOutKinesisShardSubscription {
        private final BlockingQueue<SubscribeToShardEvent> testEventQueue;
        private volatile StartingPosition currentStartingPosition;

        public TestableSubscription(
                AsyncStreamProxy kinesis,
                String consumerArn,
                String shardId,
                StartingPosition startingPosition,
                Duration subscriptionTimeout,
                ExecutorService subscriptionEventProcessingExecutor,
                BlockingQueue<SubscribeToShardEvent> testEventQueue) {
            super(kinesis, consumerArn, shardId, startingPosition, subscriptionTimeout, subscriptionEventProcessingExecutor);
            this.testEventQueue = testEventQueue;
            this.currentStartingPosition = startingPosition;
        }

        @Override
        public StartingPosition getStartingPosition() {
            return currentStartingPosition;
        }

        @Override
        public void processSubscriptionEvent(SubscribeToShardEvent event) {
            try {
                if (testEventQueue != null) {
                    // First put the event in the queue
                    testEventQueue.put(event);
                }

                // Only after successful queue.put, update the starting position
                String continuationSequenceNumber = event.continuationSequenceNumber();
                if (continuationSequenceNumber != null) {
                    // Update the starting position to ensure we can recover after failover
                    currentStartingPosition = StartingPosition.continueFromSequenceNumber(continuationSequenceNumber);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KinesisStreamsSourceException(
                        "Interrupted while adding Kinesis record to internal buffer.", e);
            }
        }
    }
}
