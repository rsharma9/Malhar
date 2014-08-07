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
package com.datatorrent.contrib.splunk;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.db.AbstractStoreOutputOperator;
import com.splunk.TcpInput;

/**
 * 
 * Base Output operator for Splunk which writes to a TCP port on which splunk server is configured.
 *
 */
public class SplunkTcpOutputOperator<T> extends AbstractStoreOutputOperator<T, SplunkStore> {

  private String tcpPort;
  private transient Socket socket;
  private transient TcpInput tcpInput;
  private transient DataOutputStream stream;

  public String getTcpPort() {

    return tcpPort;
  }
  public void setTcpPort(String tcpPort) {

    this.tcpPort = tcpPort;
  }

  @Override
  public void setup(OperatorContext context) {

    super.setup(context);
    tcpInput = (TcpInput) store.getService().getInputs().get(tcpPort);
    try {
      socket = tcpInput.attach();
      stream = new DataOutputStream(socket.getOutputStream());
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  @Override
  public void processTuple(T tuple) {

    try {
      stream.writeBytes(tuple.toString());

    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  @Override
  public void endWindow() {

    try {
      stream.flush();
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  @Override
  public void teardown() {

    super.teardown();
    try {
      socket.close();
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

}
