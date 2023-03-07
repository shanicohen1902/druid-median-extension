package io.druid.median.aggregator;

import junitparams.JUnitParamsRunner;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

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