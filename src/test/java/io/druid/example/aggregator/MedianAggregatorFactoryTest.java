package io.druid.example.aggregator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import junit.framework.TestCase;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.mean.DoubleMeanAggregatorFactory;
import org.apache.druid.query.aggregation.mean.SimpleTestIndex;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

@RunWith(JUnitParamsRunner.class)
public class MedianAggregatorFactoryTest {


    @Test
    public void testResultArraySignature()
    {
        final TimeseriesQuery query =
                Druids.newTimeseriesQueryBuilder()
                        .dataSource("dummy")
                        .intervals("2000/3000")
                        .granularity(Granularities.HOUR)
                        .aggregators(
                                new CountAggregatorFactory("count"),
                                new MedianAggregatorFactory("test","test")
                        )
                        .build();

        Assert.assertEquals(
                RowSignature.builder()
                        .addTimeColumn()
                        .add("count", ColumnType.LONG)
                        .add("test", ColumnType.FLOAT)
                        .build(),
                new TimeseriesQueryQueryToolChest().resultArraySignature(query)
        );

        System.out.println(new TimeseriesQueryQueryToolChest().resultArraySignature(query));
    }

}