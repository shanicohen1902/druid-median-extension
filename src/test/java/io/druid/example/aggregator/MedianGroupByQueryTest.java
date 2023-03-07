/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.example.aggregator;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.*;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryEngine;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MedianGroupByQueryTest extends InitializedNullHandlingTest {
    private CloseableStupidPool<ByteBuffer> pool;

    @Before
    public void setup() {
        pool = new CloseableStupidPool<>(
                "TopNQueryEngine-bufferPool",
                new Supplier<ByteBuffer>() {
                    @Override
                    public ByteBuffer get() {
                        return ByteBuffer.allocate(1024 * 1024);
                    }
                }
        );
    }

    @After
    public void teardown() {
        pool.close();
    }

    @Test
    public void testGroupByAgg() throws Exception {
        final GroupByQueryConfig config = new GroupByQueryConfig();
        final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);

        GroupByQueryEngine engine = new GroupByQueryEngine(configSupplier, pool);

        String any_double_metric = "double_metric";
        String visitor_id = "visitor_id";
        String client_type = "client_type";
        String time = "2016-03-04T00:00:00.000Z";
        DateTime dateTime = DateTimes.of(time);
        long timestamp = dateTime.getMillis();

        IncrementalIndex index = new OnheapIncrementalIndex.Builder()
                .setIndexSchema(
                        new IncrementalIndexSchema.Builder()
                                .withQueryGranularity(Granularities.DAY)
                                .withMetrics(new MedianAggregatorFactory(any_double_metric, any_double_metric))
                                .build()
                )
                .setMaxRowCount(1000)
                .build();

        index.add(
                createRow(any_double_metric,9, visitor_id, "1",client_type,  "gold" , timestamp)
        );
        index.add(
                createRow(any_double_metric,8, visitor_id, "1",client_type,  "gold", timestamp)
        );
        index.add(
                createRow(any_double_metric,7, visitor_id, "1",client_type, "gold", timestamp)
        );
        index.add(
                createRow(any_double_metric,6, visitor_id, "1",client_type,   "silver", timestamp)
        );
        index.add(
                createRow(any_double_metric,5, visitor_id, "1",client_type,   "black", timestamp)
        );
        index.add(
                createRow(any_double_metric,6, visitor_id,"1", client_type,   "black", timestamp)
        );        

        GroupByQuery query = GroupByQuery.builder()
                .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
                .setGranularity(Granularities.ALL)
                .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                .addDimension(client_type)
                .addAggregator(new MedianAggregatorFactory(any_double_metric, any_double_metric))
                .build();


        final List<Row> results =
                engine.process(query, new IncrementalIndexStorageAdapter(index)).toList();

        Assert.assertEquals(results.get(0).getMetric(any_double_metric),8.0);
        Assert.assertEquals(results.get(1).getMetric(any_double_metric),6.0);
        Assert.assertEquals(results.get(2).getMetric(any_double_metric),5.5);
    }

    private MapBasedInputRow createRow(String any_double_metric, double metric, String visitor_id, String visitorId, String client_type, String customerType, long timestamp) {
        return new MapBasedInputRow(
                timestamp,
                Lists.newArrayList(visitor_id, client_type),
                ImmutableMap.of(visitor_id, visitorId, client_type, customerType, any_double_metric, metric)
        );
    }
}
