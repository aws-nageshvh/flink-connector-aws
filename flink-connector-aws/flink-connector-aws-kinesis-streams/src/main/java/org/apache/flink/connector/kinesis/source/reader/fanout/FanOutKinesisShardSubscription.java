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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.netty.handler.timeout.ReadTimeoutException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.InternalFailureException;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FanOutSubscription class responsible for handling the subscription to a single shard of the
 * Kinesis stream. Given a shardId, it will manage the lifecycle of the subscription, and eagerly
 * keep the next batch of records available for consumption when next polled.
 */
@Internal
public class FanOutKinesisShardSubscription {
    private static final Logger LOG = LoggerFactory.getLogger(FanOutKinesisShardSubscription.class);
    private static final List<Class<? extends Throwable>> RECOVERABLE_EXCEPTIONS =
            Arrays.asList(
                    InternalFailureException.class,
                    ResourceNotFoundException.class,
                    KinesisException.class,
                    ResourceInUseException.class,
                    ReadTimeoutException.class,
                    TimeoutException.class,
                    IOException.class,
                    LimitExceededException.class);

    /** Default timeout for putting elements into the queue. */
    private static final Duration DEFAULT_PUT_TIMEOUT = Duration.ofSeconds(10);

    private final AsyncStreamProxy kinesis;
    private final String consumerArn;
    private final String shardId;

    private final Duration subscriptionTimeout;

    // Queue is meant for eager retrieval of records from the Kinesis stream.
    // Using FutureCompletingBlockingQueue for proper notification between producer and consumer
    private final FutureCompletingBlockingQueue<SubscribeToShardEvent> eventQueue;

    // Thread index for the producer (subscriber)
    private static final int PRODUCER_THREAD_INDEX = 0;
    private final AtomicBoolean subscriptionActive = new AtomicBoolean(false);
    private final AtomicReference<Throwable> subscriptionException = new AtomicReference<>();

    // Flag to indicate if we're experiencing backpressure
    private final AtomicBoolean backpressureActive = new AtomicBoolean(false);

    // Scheduler for timeout-based operations
    private final ScheduledExecutorService scheduler;

    // Store the current starting position for this subscription. Will be updated each time new
    // batch of records is consumed
    private StartingPosition startingPosition;
    private FanOutShardSubscriber shardSubscriber;

    public FanOutKinesisShardSubscription(
            AsyncStreamProxy kinesis,
            String consumerArn,
            String shardId,
            StartingPosition startingPosition,
            Duration subscriptionTimeout) {
        this.kinesis = kinesis;
        this.consumerArn = consumerArn;
        this.shardId = shardId;
        this.startingPosition = startingPosition;
        this.subscriptionTimeout = subscriptionTimeout;

        // Create a bounded queue with capacity 2
        this.eventQueue = new FutureCompletingBlockingQueue<>(2);

        // Create a scheduler for timeout operations
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new ExecutorThreadFactory("kinesis-subscription-timeout-" + shardId));
    }

    /** Method to allow eager activation of the subscription. */
    public void activateSubscription() {
        LOG.info(
                "Activating subscription to shard {} with starting position {} for consumer {}.",
                shardId,
                startingPosition,
                consumerArn);
        if (subscriptionActive.get()) {
            LOG.warn("Skipping activation of subscription since it is already active.");
            return;
        }

        // We have to use our own CountDownLatch to wait for subscription to be acquired because
        // subscription event is tracked via the handler.
        CountDownLatch waitForSubscriptionLatch = new CountDownLatch(1);
        shardSubscriber = new FanOutShardSubscriber(waitForSubscriptionLatch);
        SubscribeToShardResponseHandler responseHandler =
                SubscribeToShardResponseHandler.builder()
                        .subscriber(() -> shardSubscriber)
                        .onError(
                                throwable -> {
                                    // Errors that occur when obtaining a subscription are thrown
                                    // here.
                                    // After subscription is acquired, these errors can be ignored.
                                    if (waitForSubscriptionLatch.getCount() > 0) {
                                        terminateSubscription(throwable);
                                        waitForSubscriptionLatch.countDown();
                                    }
                                })
                        .build();

        // We don't need to keep track of the future here because we monitor subscription success
        // using our own CountDownLatch
        kinesis.subscribeToShard(consumerArn, shardId, startingPosition, responseHandler)
                .exceptionally(
                        throwable -> {
                            // If consumer exists and is still activating, we want to countdown.
                            if (ExceptionUtils.findThrowable(
                                            throwable, ResourceInUseException.class)
                                    .isPresent()) {
                                waitForSubscriptionLatch.countDown();
                                return null;
                            }
                            LOG.error(
                                    "Error subscribing to shard {} with starting position {} for consumer {}.",
                                    shardId,
                                    startingPosition,
                                    consumerArn,
                                    throwable);
                            terminateSubscription(throwable);
                            return null;
                        });

        // We have to handle timeout for subscriptions separately because Java 8 does not support a
        // fluent orTimeout() methods on CompletableFuture.
        CompletableFuture.runAsync(
                () -> {
                    try {
                        if (waitForSubscriptionLatch.await(
                                subscriptionTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                            LOG.info(
                                    "Successfully subscribed to shard {} with starting position {} for consumer {}.",
                                    shardId,
                                    startingPosition,
                                    consumerArn);
                            subscriptionActive.set(true);
                            // Request first batch of records.
                            shardSubscriber.requestRecords();
                        } else {
                            String errorMessage =
                                    "Timeout when subscribing to shard "
                                            + shardId
                                            + " with starting position "
                                            + startingPosition
                                            + " for consumer "
                                            + consumerArn
                                            + ".";
                            LOG.error(errorMessage);
                            terminateSubscription(new TimeoutException(errorMessage));
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for subscription to complete.", e);
                        terminateSubscription(e);
                        Thread.currentThread().interrupt();
                    }
                });
    }

    private void terminateSubscription(Throwable t) {
        if (!subscriptionException.compareAndSet(null, t)) {
            LOG.warn(
                    "Another subscription exception has been queued, ignoring subsequent exceptions",
                    t);
        }
        shardSubscriber.cancel();
    }

    /**
     * This is the main entrypoint for this subscription class. It will retrieve the next batch of
     * records from the Kinesis stream shard. It will throw any unrecoverable exceptions encountered
     * during the subscription process.
     *
     * @return next FanOut subscription event containing records. Returns null if subscription is
     *     not yet active and fetching should be retried at a later time.
     */
    public SubscribeToShardEvent nextEvent() {
        Throwable throwable = subscriptionException.getAndSet(null);
        if (throwable != null) {
            // If consumer is still activating, we want to wait.
            if (ExceptionUtils.findThrowable(throwable, ResourceInUseException.class).isPresent()) {
                return null;
            }
            // We don't want to wrap ResourceNotFoundExceptions because it is handled via a
            // try-catch loop
            if (throwable instanceof ResourceNotFoundException) {
                throw (ResourceNotFoundException) throwable;
            }
            Optional<? extends Throwable> recoverableException =
                    RECOVERABLE_EXCEPTIONS.stream()
                            .map(clazz -> ExceptionUtils.findThrowable(throwable, clazz))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .findFirst();
            if (recoverableException.isPresent()) {
                LOG.warn(
                        "Recoverable exception encountered while subscribing to shard. Ignoring.",
                        recoverableException.get());
                shardSubscriber.cancel();
                activateSubscription();
                return null;
            }
            LOG.error("Subscription encountered unrecoverable exception.", throwable);
            throw new KinesisStreamsSourceException(
                    "Subscription encountered unrecoverable exception.", throwable);
        }

        if (!subscriptionActive.get()) {
            LOG.debug(
                    "Subscription to shard {} for consumer {} is not yet active. Skipping.",
                    shardId,
                    consumerArn);
            return null;
        }

        SubscribeToShardEvent event = eventQueue.poll();

        // If we got an event and backpressure was active, we can deactivate it now
        if (event != null && backpressureActive.compareAndSet(true, false)) {
            LOG.info("Backpressure relieved for shard {}", shardId);
        }

        // Request next batch of records after consumption
        // if (event != null && shardSubscriber != null && !backpressureActive.get()) {
        //    shardSubscriber.requestRecords();
        //}

        return event;
    }

    /**
     * Returns the availability future for the event queue. This can be used by consumers
     * to be notified when new events are available.
     *
     * @return a future that completes when events are available
     */
    public CompletableFuture<Void> getAvailabilityFuture() {
        return eventQueue.getAvailabilityFuture();
    }

    /**
     * Checks if there are events available in the queue.
     *
     * @return true if the queue is not empty, false otherwise
     */
    public boolean hasEvents() {
        return !eventQueue.isEmpty();
    }

    /**
     * Wakes up the producer thread if it's blocked in putting elements into the queue.
     * This is typically called during shutdown or when a split is being cancelled.
     */
    public void wakeUpProducer() {
        LOG.info("Waking up producer thread for shard {}", shardId);
        eventQueue.wakeUpPuttingThread(PRODUCER_THREAD_INDEX);
    }

    /**
     * Cancels the subscription and wakes up any blocked producer threads.
     */
    public void cancelSubscription() {
        LOG.info("NVH: Cancelling subscription to shard {} for consumer {}", shardId, consumerArn);

        // Wake up any blocked producer threads
        wakeUpProducer();

        // Then cancel the subscription
        if (shardSubscriber != null) {
            shardSubscriber.cancel();
        }

        // Shut down the scheduler
        scheduler.shutdownNow();
    }

    /**
     * Gets the event queue for this subscription.
     *
     * @return the event queue
     */
    public FutureCompletingBlockingQueue<SubscribeToShardEvent> getEventQueue() {
        return eventQueue;
    }

    /**
     * Implementation of {@link Subscriber} to retrieve events from Kinesis stream using Reactive
     * Streams.
     */
    private class FanOutShardSubscriber implements Subscriber<SubscribeToShardEventStream> {
        private final CountDownLatch subscriptionLatch;

        private Subscription subscription;

        private FanOutShardSubscriber(CountDownLatch subscriptionLatch) {
            this.subscriptionLatch = subscriptionLatch;
        }

        public void requestRecords() {
            if (!backpressureActive.get()) {
                subscription.request(1);
            } else {
                LOG.debug("Skipping record request due to active backpressure for shard {}", shardId);
            }
        }

        public void cancel() {
            if (!subscriptionActive.get()) {
                LOG.warn("NVH: Trying to cancel inactive subscription. Ignoring.");
                return;
            }
            LOG.info("NVH: Cancelling subscription to shard {} for consumer {}", shardId, consumerArn);
            subscriptionActive.set(false);
            if (subscription != null) {
                subscription.cancel();
            }
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            LOG.info(
                    "Successfully subscribed to shard {} at {} using consumer {}.",
                    shardId,
                    startingPosition,
                    consumerArn);
            this.subscription = subscription;
            subscriptionLatch.countDown();
        }

        @Override
        public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
            subscribeToShardEventStream.accept(
                    new SubscribeToShardResponseHandler.Visitor() {
                        @Override
                        public void visit(SubscribeToShardEvent event) {
                            try {
                                LOG.debug(
                                        "Received event: {}, {}",
                                        event.getClass().getSimpleName(),
                                        event);

                                // Try to put the event with a timeout
                                putWithTimeout(event, DEFAULT_PUT_TIMEOUT);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new KinesisStreamsSourceException(
                                        "Interrupted while adding Kinesis record to internal buffer.",
                                        e);
                            }
                        }
                    });
        }

        /**
         * Puts an event into the queue with a timeout. If the timeout expires, it wakes up the
         * producer thread and activates backpressure.
         *
         * @param event the event to put
         * @param timeout the timeout duration
         * @throws InterruptedException if the thread is interrupted
         */
        private void putWithTimeout(SubscribeToShardEvent event, Duration timeout) throws InterruptedException {
            // Schedule a task to wake up the producer thread after the timeout
            ScheduledExecutorService scheduler = FanOutKinesisShardSubscription.this.scheduler;
            final CompletableFuture<Void> timeoutFuture = new CompletableFuture<>();

            // Schedule a task to wake up the producer thread after the timeout
            scheduler.schedule(() -> {
                LOG.warn("NVH: Put timeout expired for shard {}, waking up producer thread", shardId);
                eventQueue.wakeUpPuttingThread(PRODUCER_THREAD_INDEX);
                timeoutFuture.complete(null);
                return null;
            }, timeout.toMillis(), TimeUnit.MILLISECONDS);

            try {
                // Try to put the event
                boolean added = eventQueue.put(PRODUCER_THREAD_INDEX, event);

                // Cancel the timeout task if we succeeded or failed
                timeoutFuture.cancel(false);

                if (!added) {
                    LOG.warn("NVH: Failed to add event to queue for shard {} due to wakeup", shardId);

                    // Activate backpressure to stop requesting more records
                    if (backpressureActive.compareAndSet(false, true)) {
                        LOG.info("NVH: Activating backpressure for shard {}", shardId);
                    }

                    return;
                }

                // Update the starting position in case we have to recreate the subscription
                startingPosition = StartingPosition.continueFromSequenceNumber(
                        event.continuationSequenceNumber());

                shardSubscriber.requestRecords();

                // Don't request records here - we'll request after consumption
            } finally {
                // Make sure we cancel the timeout task
                timeoutFuture.cancel(false);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (!subscriptionException.compareAndSet(null, throwable)) {
                LOG.warn(
                        "Another subscription exception has been queued, ignoring subsequent exceptions",
                        throwable);
            }
        }

        @Override
        public void onComplete() {
            LOG.info("Subscription complete - {} ({})", shardId, consumerArn);
            cancel();
            activateSubscription();
        }
    }
}
