/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.realtime.impl.kafka;

import java.util.Map;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;


/**
 * Low level stream config, adds some overrides for llc-specific properties.
 */
public class KafkaLowLevelStreamProviderConfig extends KafkaHighLevelStreamProviderConfig {
  private static final int NOT_DEFINED = Integer.MIN_VALUE;

  private long llcSegmentTimeInMillis = NOT_DEFINED;
  private int llcRealtimeRecordsThreshold = NOT_DEFINED;
  public static final String LLC_PROPERTY_SUFFIX = ".llc";

  @Override
  public void init(AbstractTableConfig tableConfig, InstanceZKMetadata instanceMetadata, Schema schema) {
    super.init(tableConfig, instanceMetadata, schema);

    if (tableConfig.getIndexingConfig().getStreamConfigs().containsKey(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE + LLC_PROPERTY_SUFFIX)) {
      llcRealtimeRecordsThreshold =
          Integer.parseInt(tableConfig.getIndexingConfig().getStreamConfigs().get(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE + LLC_PROPERTY_SUFFIX));
    }

    if (tableConfig.getIndexingConfig().getStreamConfigs().containsKey(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME + LLC_PROPERTY_SUFFIX)) {
      llcSegmentTimeInMillis =
          Long.parseLong(tableConfig.getIndexingConfig().getStreamConfigs().get(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME + LLC_PROPERTY_SUFFIX));
    }
  }

  @Override
  public void init(Map<String, String> properties, Schema schema) {
    super.init(properties, schema);

    if (properties.containsKey(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE + LLC_PROPERTY_SUFFIX)) {
      llcRealtimeRecordsThreshold =
          Integer.parseInt(properties.get(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE + LLC_PROPERTY_SUFFIX));
    }

    if (properties.containsKey(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME + LLC_PROPERTY_SUFFIX)) {
      llcSegmentTimeInMillis =
          convertToMs(properties.get(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME + LLC_PROPERTY_SUFFIX));
    }
  }

  @Override
  public int getSizeThresholdToFlushSegment() {
    if (llcRealtimeRecordsThreshold != NOT_DEFINED) {
      return llcRealtimeRecordsThreshold;
    } else {
      return super.getSizeThresholdToFlushSegment();
    }
  }

  @Override
  public long getTimeThresholdToFlushSegment() {
    if (llcSegmentTimeInMillis != NOT_DEFINED) {
      return llcSegmentTimeInMillis;
    } else {
      return super.getTimeThresholdToFlushSegment();
    }
  }
}
