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
package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LLCSegmentBaseClass extends PinotSegmentUploadRestletResource {
  private static Logger LOGGER = LoggerFactory.getLogger(LLCSegmentBaseClass.class);

  long _offset;
  String _segmentNameStr;
  String _instanceId;

  public LLCSegmentBaseClass() throws IOException {
  }

  Long getOffset() {
    return _offset;
  }

  String getSegmentNameStr() {
    return _segmentNameStr;
  }

  String getInstanceId() {
    return _instanceId;
  }

  boolean extractParams() {
    final String offsetStr = getReference().getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_OFFSET);
    final String segmentName = getReference().getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_SEGMENT_NAME);
    final String instanceId = getReference().getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_INSTANCE_ID);

    if (offsetStr == null || segmentName == null || instanceId == null) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offsetStr, segmentName, instanceId);
      return false;
    }
    _segmentNameStr = segmentName;
    _instanceId = instanceId;
    try {
      _offset = Long.valueOf(offsetStr);
    } catch (NumberFormatException e) {
      LOGGER.error("Invalid offset {} for segment {} from instance {}", offsetStr, segmentName, instanceId);
      return false;
    }
    return true;
  }
}
