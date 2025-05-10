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

package org.apache.flink.connector.kinesis.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.kinesis.source.event.SplitsFinishedEvent;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates the reading from assigned splits. Runs on the TaskManager.
 *
 * @param <T> the data type emitted by the Kinesis stream source
 */
@Internal
public class KinesisStreamsSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                Record, T, KinesisShardSplit, KinesisShardSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamsSourceReader.class);
    private final Map<String, KinesisShardMetrics> shardMetricGroupMap;
    private ScheduledExecutorService logScheduler;

    public KinesisStreamsSourceReader(
            SingleThreadFetcherManager<Record, KinesisShardSplit> splitFetcherManager,
            RecordEmitter<Record, T, KinesisShardSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            Map<String, KinesisShardMetrics> shardMetricGroupMap) {
        super(splitFetcherManager, recordEmitter, config, context);
        this.shardMetricGroupMap = shardMetricGroupMap;

        // Start a scheduled task to log elementsQueue capacity
        this.logScheduler = new ScheduledThreadPoolExecutor(1);
        this.logScheduler.scheduleAtFixedRate(
                this::logElementsQueueCapacity, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    protected void onSplitFinished(Map<String, KinesisShardSplitState> finishedSplitIds) {
        context.sendSourceEventToCoordinator(
                new SplitsFinishedEvent(new HashSet<>(finishedSplitIds.keySet())));
        finishedSplitIds.keySet().forEach(this::unregisterShardMetricGroup);
    }

    @Override
    protected KinesisShardSplitState initializedState(KinesisShardSplit split) {
        return new KinesisShardSplitState(split);
    }

    @Override
    protected KinesisShardSplit toSplitType(String splitId, KinesisShardSplitState splitState) {
        return splitState.getKinesisShardSplit();
    }

    @Override
    public void addSplits(List<KinesisShardSplit> splits) {
        splits.forEach(this::registerShardMetricGroup);
        super.addSplits(splits);
    }

    /**
     * Logs the capacity of the elementsQueue.
     */
    private void logElementsQueueCapacity() {
        // Access the elementsQueue through the splitFetcherManager
        if (splitFetcherManager != null) {
            try {
                // Get the queue from the splitFetcherManager
                org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue<?> queue =
                        splitFetcherManager.getQueue();
                if (queue != null) {
                    LOG.warn(
                            "NVH: elementsQueue [consumer] - size: {}, remaining: {}, capacity: {}",
                            queue.size(),
                            queue.remainingCapacity(),
                            queue.size() + queue.remainingCapacity());
                }
            } catch (Exception e) {
                LOG.warn("Error logging elementsQueue capacity", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (logScheduler != null) {
            logScheduler.shutdown();
            try {
                if (!logScheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    logScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                logScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        super.close();
        this.shardMetricGroupMap.keySet().forEach(this::unregisterShardMetricGroup);
    }

    private void registerShardMetricGroup(KinesisShardSplit split) {
        if (!this.shardMetricGroupMap.containsKey(split.getShardId())) {
            this.shardMetricGroupMap.put(
                    split.getShardId(), new KinesisShardMetrics(split, context.metricGroup()));
        } else {
            LOG.warn(
                    "Metric group for shard with id {} has already been registered.",
                    split.getShardId());
        }
    }

    private void unregisterShardMetricGroup(String shardId) {
        KinesisShardMetrics removed = this.shardMetricGroupMap.remove(shardId);
        if (removed != null) {
            removed.unregister();
        } else {
            LOG.warn(
                    "Shard metric group unregister failed. Metric group for {} does not exist.",
                    shardId);
        }
    }
}
