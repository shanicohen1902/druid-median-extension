/*
 * Copyright 2019 Imply Data, Inc.
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

package io.druid.example.aggregator;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public class MedianAggregator implements Aggregator
{
  private final BaseFloatColumnValueSelector selector;

  private Queue<Float> minHeap, maxHeap;

  public MedianAggregator(BaseFloatColumnValueSelector selector)
  {
    this.selector = selector;

    minHeap = new PriorityQueue<>();
    maxHeap = new PriorityQueue<>(Comparator.reverseOrder());
  }

  @Override
  public void aggregate()
  {
    float num = selector.getFloat();
    if (minHeap.size() == maxHeap.size()) {
      maxHeap.offer(num);
      minHeap.offer(maxHeap.poll());
    } else {
      minHeap.offer(num);
      maxHeap.offer(minHeap.poll());
    }
  }

  @Override
  public Object get()
  {
    int minHeapSize = minHeap.size();
    int maxHeapSize = maxHeap.size();

    float median = 0.0f;
    if (minHeapSize > maxHeapSize) {
      median = minHeap.peek();
    } else if(maxHeapSize > 0){
      median = (minHeap.peek() + maxHeap.peek()) / 2;
    }
    return median;
  }

  @Override
  public float getFloat()
  {
    return (float) get();
  }

  @Override
  public long getLong()
  {
    return (long) get();
  }

  @Override
  public Aggregator clone()
  {
    return new MedianAggregator(selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
