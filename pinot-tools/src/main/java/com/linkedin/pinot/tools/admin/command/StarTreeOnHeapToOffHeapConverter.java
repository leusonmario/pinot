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
package com.linkedin.pinot.tools.admin.command;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.startree.StarTreeInterf;
import com.linkedin.pinot.core.startree.StarTreeSerDe;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the Star Tree ON_HEAP to OFF_HEAP converter.
 */
public class StarTreeOnHeapToOffHeapConverter extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeOnHeapToOffHeapConverter.class);

  @Option(name = "-segmentDir", required = true, metaVar = "<String>", usage = "path to untarred input segment.")
  private String _segmentDir;

  @Option(name = "-outputDir", required = true, metaVar = "<String>", usage = "output directory for new segment")
  private String _outputDir;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean execute()
      throws Exception {
    File indexDir = new File(_segmentDir);
    long start = System.currentTimeMillis();

    LOGGER.info("Loading segment {}", indexDir.getName());
    IndexSegment segment = Loaders.IndexSegment.load(indexDir, ReadMode.heap);

    long end = System.currentTimeMillis();
    LOGGER.info("Loaded segment {} in {} ms ", indexDir.getName(), (end - start));

    start = end;
    StarTreeInterf starTreeOnHeap = segment.getStarTree();
    File starTreeOffHeapFile = new File(TMP_DIR, (V1Constants.STAR_TREE_INDEX_FILE + System.currentTimeMillis()));

    // Convert the star tree on-heap to off-heap format.
    StarTreeSerDe.writeTreeOffHeapFormat(starTreeOnHeap, starTreeOffHeapFile);

    // Copy all the indexes into output directory.
    File outputDir = new File(_outputDir);
    FileUtils.deleteQuietly(outputDir);
    FileUtils.copyDirectory(indexDir, outputDir);

    // Delete the existing star tree on-heap file from the output directory.
    FileUtils.deleteQuietly(new File(_outputDir, V1Constants.STAR_TREE_INDEX_FILE));

    // Move the temp star tree off-heap file into the output directory.
    FileUtils.moveFile(starTreeOffHeapFile, new File(_outputDir, V1Constants.STAR_TREE_INDEX_FILE));
    end = System.currentTimeMillis();

    LOGGER.info("Converted segment: {} ms", (end - start));
    return true;
  }

  @Override
  public String description() {
    return "Convert Pinto Segment with Star Tree ON_HEAP format into Pinot Segment with Star Tree OFF_HEAP format";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String toString() {
    return "StarTreeOnHeapToOffHeapConverter -segmentDir " + _segmentDir + " -outputDir " + _outputDir;
  }

  public String getSegmentDir() {
    return _segmentDir;
  }

  public StarTreeOnHeapToOffHeapConverter setSegmentDir(String segmentDir) {
    _segmentDir = segmentDir;
    return this;
  }

  public String getOutputDir() {
    return _outputDir;
  }

  public StarTreeOnHeapToOffHeapConverter setOutputDir(String outputDir) {
    _outputDir = outputDir;
    return this;
  }
}
