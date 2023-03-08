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

package io.druid.median.aggregator;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.groupby.GroupByQueryConfig;
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
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MedianTopNQueryTest extends InitializedNullHandlingTest
{
  private CloseableStupidPool<ByteBuffer> pool;

  @Before
  public void setup()
  {
    pool = new CloseableStupidPool<>(
        "TopNQueryEngine-bufferPool",
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        }
    );
  }

  @After
  public void teardown()
  {
    pool.close();
  }

  @Test
  public void testTopNWithMedianAgg() throws Exception
  {
    TopNQueryEngine engine = new TopNQueryEngine(pool);
      final GroupByQueryConfig config = new GroupByQueryConfig();

      String double_metric = "double_metric";
      String visitor_id = "visitor_id";
      String client_type = "client_type";
      DateTime time = DateTimes.of("2016-03-04T00:00:00.000Z");
      long timestamp = time.getMillis();

    IncrementalIndex index = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.DAY)
                .withMetrics(new MedianAggregatorFactory(double_metric, double_metric))
                .build()
        )
        .setMaxRowCount(1000)
        .build();

    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.of(visitor_id, "1", client_type, "iphone", double_metric, 6)
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.of(visitor_id, "1", client_type, "iphone", double_metric, 7)
        )
    );
      index.add(
              new MapBasedInputRow(
                      timestamp,
                      Lists.newArrayList(visitor_id, client_type),
                      ImmutableMap.of(visitor_id, "1", client_type, "android", double_metric, 8)
              )
      );

    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.of(visitor_id, "2", client_type, "android", double_metric, 10)
        )
    );

    TopNQuery query = new TopNQueryBuilder().dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                          .granularity(QueryRunnerTestHelper.DAY_GRAN)
                          .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                          .dimension(client_type)
                          .metric(double_metric)
                          .threshold(3)
                          .aggregators(
                              QueryRunnerTestHelper.ROWS_COUNT,
                              new MedianAggregatorFactory(double_metric, double_metric)
                          )
                          .build();


    final Iterable<Result<TopNResultValue>> results =
        engine.query(query, new IncrementalIndexStorageAdapter(index), null).toList();


    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            time,
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                        ImmutableMap.of(
                                client_type, "android",
                                double_metric, 9.0,
                                "rows", 2L
                        ),
                    ImmutableMap.of(
                        client_type, "iphone",
                        double_metric, 6.5,
                        "rows", 1L
                    )
                )
            )
        )
    );
    results.forEach(r -> r.getValue().forEach( t -> System.out.println(t)));
    TestHelper.assertExpectedResults(expectedResults, results);
  }
}
