/*
 * Copyright 2016 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.median.aggregator;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.PriorityQueue;
import java.util.Queue;

public class MedianBufferAggregator implements BufferAggregator
{
  private final BaseFloatColumnValueSelector selector;

  private final Int2ObjectMap<Queue<Float>> minHeapCollection = new Int2ObjectOpenHashMap<>();
  private final Int2ObjectMap<Queue<Float>> maxHeapCollection = new Int2ObjectOpenHashMap<>();

  MedianBufferAggregator(BaseFloatColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    buf.putDouble(position, 0.0d);
  }

  @Override
  public final void aggregate(ByteBuffer buf, int position)
  {
    Queue<Float> minHeap = getMinHeapCollection(position);
    Queue<Float> maxHeap = getMaxHeapCollection(position);

    float num = selector.getFloat();
    if (minHeap.size() == maxHeap.size()) {
      maxHeap.offer(num);
      minHeap.offer(maxHeap.poll());
    } else {
      minHeap.offer(num);
      maxHeap.offer(minHeap.poll());
    }

    float median = 0;
    if (minHeap.size() > maxHeap.size()) {
      median = minHeap.peek();
    } else if(maxHeap.size() > 0){
      median = (minHeap.peek() + maxHeap.peek()) / 2;
    }

    buf.putDouble(position, median);
  }

  @Override
  public final Object get(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public final float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getDouble(position);
  }

  @Override
  public final long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getDouble(position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  private Queue<Float> getMinHeapCollection(int position)
  {
    Queue<Float> minHeap = minHeapCollection.get(position);
    if (minHeap == null) {
      minHeap = new PriorityQueue<>();
      minHeapCollection.put(position, minHeap);
    }
    return minHeap;
  }

  private Queue<Float> getMaxHeapCollection(int position)
  {
    Queue<Float> maxHeap = maxHeapCollection.get(position);
    if (maxHeap == null) {
      maxHeap = new PriorityQueue<>();
      maxHeapCollection.put(position, maxHeap);
    }
    return maxHeap;
  }
}
