package io.druid.median.aggregator;

import junit.framework.TestCase;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class MedianAggregatorTest extends TestCase {

    private ColumnSelectorFactory selectorFactory;
    private TestObjectColumnSelector selector;


    @Test
    public void testAggregator()
    {
        Float[] values = new Float[2];
        values[0] = 9.7f;
        values[1] = 0.1f;
        selector = new TestObjectColumnSelector<>(values);
        selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
        EasyMock.expect(selectorFactory.makeColumnValueSelector("test")).andReturn(selector);
        EasyMock.replay(selectorFactory);

        MedianAggregatorFactory medianAggregatorFactory = new MedianAggregatorFactory("test","test");
        MedianAggregator aggregator = (MedianAggregator) medianAggregatorFactory.factorize(selectorFactory);

        Assert.assertEquals(0.0f, aggregator.get());

        for (Float value : values) {
            aggregate(selector, aggregator);
        }

        assertEquals(4.9f, aggregator.getDouble(), 0.001);
    }

    @Test
    public void testAggregatorDouble()
    {
        Double[] values = new Double[2];
        values[0] = 9.7d;
        values[1] = 0.1d;
        selector = new TestObjectColumnSelector<>(values);
        selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
        EasyMock.expect(selectorFactory.makeColumnValueSelector("test")).andReturn(selector);
        EasyMock.replay(selectorFactory);

        MedianAggregatorFactory medianAggregatorFactory = new MedianAggregatorFactory("test","test");
        MedianAggregator aggregator = (MedianAggregator) medianAggregatorFactory.factorize(selectorFactory);

        Assert.assertEquals(0.0f, aggregator.get());

        for (Double value : values) {
            aggregate(selector, aggregator);
        }

        assertEquals(4.9f, aggregator.getDouble(), 0.001);
    }

    private void aggregate(TestObjectColumnSelector selector, MedianAggregator agg)
    {
        agg.aggregate();
        selector.increment();
    }

    @Test
    public void testBufferAggregator()
    {
        Float[] values = new Float[2];
        values[0] = 9.7f;
        values[1] = 0.1f;
        selector = new TestObjectColumnSelector<>(values);
        selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
        EasyMock.expect(selectorFactory.makeColumnValueSelector("test")).andReturn(selector);
        EasyMock.replay(selectorFactory);

        MedianAggregatorFactory medianAggregatorFactory = new MedianAggregatorFactory("test","test");
        MedianBufferAggregator aggregator = (MedianBufferAggregator) medianAggregatorFactory.factorizeBuffered(selectorFactory);

        ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        aggregator.init(buffer, 0);

        for (Float value : values) {
            aggregate(selector, aggregator, buffer, 0);
        }

        assertEquals(4.9, aggregator.getDouble(buffer, 0), 0.01);
    }

    @Test
    public void testBufferAggregatorSingleValue()
    {
        Float[] values = new Float[1];
        values[0] = 9.7f;
        selector = new TestObjectColumnSelector<>(values);
        selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
        EasyMock.expect(selectorFactory.makeColumnValueSelector("test")).andReturn(selector);
        EasyMock.replay(selectorFactory);

        MedianAggregatorFactory medianAggregatorFactory = new MedianAggregatorFactory("test","test");
        MedianBufferAggregator aggregator = (MedianBufferAggregator) medianAggregatorFactory.factorizeBuffered(selectorFactory);

        ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        aggregator.init(buffer, 0);

        for (Float value : values) {
            aggregate(selector, aggregator, buffer, 0);
        }

        assertEquals(9.7, aggregator.getDouble(buffer, 0), 0.01);
    }

    @Test
    public void testBufferAggregatorZero()
    {
        Float[] values = new Float[10];
        values[1] = 9.7f;
        selector = new TestObjectColumnSelector<>(values);
        selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
        EasyMock.expect(selectorFactory.makeColumnValueSelector("test")).andReturn(selector);
        EasyMock.replay(selectorFactory);

        MedianAggregatorFactory medianAggregatorFactory = new MedianAggregatorFactory("test","test");
        MedianBufferAggregator aggregator = (MedianBufferAggregator) medianAggregatorFactory.factorizeBuffered(selectorFactory);

        ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        aggregator.init(buffer, 0);

        for (Float value : values) {
            aggregate(selector, aggregator, buffer, 0);
        }

        assertEquals(0, aggregator.getDouble(buffer, 0), 0.01);
    }

    private void aggregate(TestObjectColumnSelector selector, MedianBufferAggregator agg, ByteBuffer buf, int pos)
    {
        agg.aggregate(buf, pos);
        selector.increment();
    }

}