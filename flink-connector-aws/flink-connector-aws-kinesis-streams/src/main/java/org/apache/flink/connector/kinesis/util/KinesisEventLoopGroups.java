/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.util;

import org.apache.flink.annotation.Internal;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

/**
 * Utility class for managing separate event loop groups for Kinesis source and sink connectors.
 * This prevents them from sharing the same event loop group, which can help isolate their network operations.
 */
@Internal
public class KinesisEventLoopGroups {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisEventLoopGroups.class);

    private static final int DEFAULT_NUM_THREADS = 1; // Let Netty decide based on available processors

    // Singleton instances for source and sink event loop groups
    private static volatile SdkEventLoopGroup sourceEventLoopGroup;
    private static volatile SdkEventLoopGroup sinkEventLoopGroup;

    private KinesisEventLoopGroups() {
        // Utility class, no public constructor
    }

    /**
     * Gets the event loop group for the Kinesis source connector.
     * Creates it if it doesn't exist yet.
     *
     * @return The source event loop group
     */
    public static synchronized SdkEventLoopGroup getSourceEventLoopGroup() {
        if (sourceEventLoopGroup == null) {
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                    .threadNamePrefix("NVH-kinesis-source-event-loop")
                    .build();
            EventLoopGroup eventLoopGroup = new NioEventLoopGroup(DEFAULT_NUM_THREADS, threadFactory);
            sourceEventLoopGroup = SdkEventLoopGroup.create(eventLoopGroup);
            LOG.info("NVH: Created dedicated event loop group for Kinesis source connector");
        }
        return sourceEventLoopGroup;
    }

    /**
     * Gets the event loop group for the Kinesis sink connector.
     * Creates it if it doesn't exist yet.
     *
     * @return The sink event loop group
     */
    public static synchronized SdkEventLoopGroup getSinkEventLoopGroup() {
        if (sinkEventLoopGroup == null) {
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                    .threadNamePrefix("NVH-kinesis-sink-event-loop")
                    .build();
            EventLoopGroup eventLoopGroup = new NioEventLoopGroup(DEFAULT_NUM_THREADS, threadFactory);
            sinkEventLoopGroup = SdkEventLoopGroup.create(eventLoopGroup);
            LOG.info("NVH: Created dedicated event loop group for Kinesis sink connector");
        }
        return sinkEventLoopGroup;
    }

    /**
     * Configures a NettyNioAsyncHttpClient builder to use the source event loop group.
     *
     * @param builder The builder to configure
     * @return The configured builder
     */
    public static NettyNioAsyncHttpClient.Builder configureSourceHttpClientBuilder(
            NettyNioAsyncHttpClient.Builder builder) {
        return builder.eventLoopGroup(getSourceEventLoopGroup());
    }

    /**
     * Configures a NettyNioAsyncHttpClient builder to use the sink event loop group.
     *
     * @param builder The builder to configure
     * @return The configured builder
     */
    public static NettyNioAsyncHttpClient.Builder configureSinkHttpClientBuilder(
            NettyNioAsyncHttpClient.Builder builder) {
        return builder.eventLoopGroup(getSinkEventLoopGroup());
    }

    /**
     * Closes the source and sink event loop groups.
     * This should be called when the application is shutting down.
     */
    public static synchronized void closeEventLoopGroups() {
        if (sourceEventLoopGroup != null) {
            try {
                sourceEventLoopGroup.eventLoopGroup().shutdownGracefully().sync();
                LOG.info("NVH: Closed source event loop group");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("NVH: Interrupted while closing source event loop group", e);
            } finally {
                sourceEventLoopGroup = null;
            }
        }

        if (sinkEventLoopGroup != null) {
            try {
                sinkEventLoopGroup.eventLoopGroup().shutdownGracefully().sync();
                LOG.info("NVH: Closed sink event loop group");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("NVH: Interrupted while closing sink event loop group", e);
            } finally {
                sinkEventLoopGroup = null;
            }
        }
    }
}
