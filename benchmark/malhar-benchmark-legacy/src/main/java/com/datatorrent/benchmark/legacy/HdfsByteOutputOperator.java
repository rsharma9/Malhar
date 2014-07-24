/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.benchmark.legacy;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractHdfsRollingFileOutputOperator;

/**
 * Adapter for writing byte arrays to HDFS
 * <p>
 * Serializes tuples into a HDFS file.<br/>
 * </p>
 *
 * @since 0.9.4
 */
public class HdfsByteOutputOperator extends AbstractHdfsRollingFileOutputOperator<byte[]>
{

  /**
   * File name substitution parameter: The system assigned id of the operator instance, which is unique for the
   * application.
   */
  public static final String FNAME_SUB_CONTEXT_ID = "contextId";
  /**
   * File name substitution parameter: Index of part file when a file size limit is specified.
   */
  public static final String FNAME_SUB_PART_INDEX = "partIndex";

  private int contextId;
  private int index = 0;

  @Override
  public Path nextFilePath()
  {
    Map<String, String> params = new HashMap<String, String>();
    params.put(FNAME_SUB_PART_INDEX, String.valueOf(index));
    params.put(FNAME_SUB_CONTEXT_ID, Integer.toString(contextId));
    StrSubstitutor sub = new StrSubstitutor(params, "%(", ")");
    index++;
    return new Path(sub.replace(getFilePath().toString()));
  }

  @Override
  public void setup(OperatorContext context)
  {
    contextId = context.getId();
    super.setup(context);
  }

  @Override
  public byte[] getBytesForTuple(byte[] t)
  {
    return t;
  }

}
