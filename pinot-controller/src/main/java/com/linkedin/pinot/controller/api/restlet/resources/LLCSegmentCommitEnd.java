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
import com.linkedin.pinot.common.restlet.swagger.Description;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LLCSegmentCommitEnd extends ServerResource {
  private static Logger LOGGER = LoggerFactory.getLogger(LLCSegmentCommitStart.class);
  long _offset;
  String _segmentNameStr;
  String _instanceId;

  @HttpVerb("get")
  @Description("Segment commit end for a LLC segment coming in from a server")
  @Summary("Segment commit end for a LLC segment coming in from a server")
  @Paths({"/" + SegmentCompletionProtocol.MSG_TYPE_COMMIT_END})
  public Representation get() {
    if (!extractParams()) {
      return new StringRepresentation(SegmentCompletionProtocol.RESP_FAILED.toJsonString());
    }
    LOGGER.info("segment={} offset={} instance={} ", _segmentNameStr, _offset, _instanceId);

    final SegmentCompletionProtocol.Request.Params reqParams = new SegmentCompletionProtocol.Request.Params();
    reqParams.withInstanceId(_instanceId).withSegmentName(_segmentNameStr).withOffset(_offset);
    SegmentCompletionProtocol.Response response = SegmentCompletionManager.getInstance().segmentCommitEnd(reqParams, true, true);
    LOGGER.info("Response: instance={}  segment={} status={} offset={}", _instanceId, _segmentNameStr,
        response.getStatus(), response.getOffset());
    return new StringRepresentation(response.toJsonString());
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
