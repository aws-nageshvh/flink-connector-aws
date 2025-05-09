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
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.reader.KinesisShardSplitReaderBase;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of the KinesisShardSplitReader that consumes from Kinesis using Enhanced
 * Fan-Out and HTTP/2.
 */
@Internal
public class FanOutKinesisShardSplitReader extends KinesisShardSplitReaderBase {
    private static final Logger LOG = LoggerFactory.getLogger(FanOutKinesisShardSplitReader.class);
    private final AsyncStreamProxy asyncStreamProxy;
    private final String consumerArn;
    private final Duration subscriptionTimeout;

    private final Map<String, FanOutKinesisShardSubscription> splitSubscriptions = new HashMap<>();

    public FanOutKinesisShardSplitReader(
            AsyncStreamProxy asyncStreamProxy,
            String consumerArn,
            Map<String, KinesisShardMetrics> shardMetricGroupMap,
            Duration subscriptionTimeout) {
        super(shardMetricGroupMap);
        this.asyncStreamProxy = asyncStreamProxy;
        this.consumerArn = consumerArn;
        this.subscriptionTimeout = subscriptionTimeout;
    }

    @Override
    protected RecordBatch fetchRecords(KinesisShardSplitState splitState) {
        String shardId = splitState.getShardId();
        FanOutKinesisShardSubscription subscription = splitSubscriptions.get(shardId);

        if (subscription == null) {
            return null;
        }

        // Check if subscription has events before trying to fetch
        if (!subscription.hasEvents()) {
            return null;
        }

        SubscribeToShardEvent event = subscription.nextEvent();
        if (event == null) {
            return null;
        }

        boolean shardCompleted = event.continuationSequenceNumber() == null;
        if (shardCompleted) {
            splitSubscriptions.remove(shardId);
        }
        return new RecordBatch(event.records(), event.millisBehindLatest(), shardCompleted);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KinesisShardSplit> splitsChanges) {
        super.handleSplitsChanges(splitsChanges);

        for (KinesisShardSplit split : splitsChanges.splits()) {
            String shardId = split.getShardId();

            FanOutKinesisShardSubscription subscription =
                    new FanOutKinesisShardSubscription(
                            asyncStreamProxy,
                            consumerArn,
                            shardId,
                            split.getStartingPosition(),
                            subscriptionTimeout);

            subscription.activateSubscription();
            splitSubscriptions.put(split.splitId(), subscription);
        }
    }

    /**
     * Returns true if any of the subscriptions has events available.
     *
     * @return true if any subscription has events, false otherwise
     */
    public boolean hasEvents() {
        for (Map.Entry<String, FanOutKinesisShardSubscription> entry : splitSubscriptions.entrySet()) {
            if (entry.getValue().hasEvents()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        // Wake up any blocked producer threads in all subscriptions
        for (FanOutKinesisShardSubscription subscription : splitSubscriptions.values()) {
            subscription.cancelSubscription();
        }

        // Then close resources
        asyncStreamProxy.close();
    }

    /**
     * Handles splits to remove by cancelling their subscriptions and waking up any blocked threads.
     *
     * @param splitsToRemove the splits to remove
     */
    public void handleSplitsToRemove(Collection<KinesisShardSplit> splitsToRemove) {
        for (KinesisShardSplit split : splitsToRemove) {
            FanOutKinesisShardSubscription subscription = splitSubscriptions.get(split.splitId());
            if (subscription != null) {
                subscription.cancelSubscription();
                splitSubscriptions.remove(split.splitId());
            }
        }
    }
}
