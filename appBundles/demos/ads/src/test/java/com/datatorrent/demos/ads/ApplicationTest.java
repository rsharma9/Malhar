/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.ads;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.ads.Application;
import java.io.IOException;
import javax.validation.ConstraintViolationException;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  @Test
  public void testJavaConfig() throws IOException, Exception {
    LocalMode lma = LocalMode.newInstance();

    Application app = new Application();
    app.setUnitTestMode(); // terminate quickly
    //app.setLocalMode(); // terminate with a long run
    app.populateDAG(lma.getDAG(), new Configuration(false));
    try {
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(20000);
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
