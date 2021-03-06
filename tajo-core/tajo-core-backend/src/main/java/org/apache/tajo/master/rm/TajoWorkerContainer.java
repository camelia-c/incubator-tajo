/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.master.rm;

import org.apache.hadoop.yarn.api.records.*;

public class TajoWorkerContainer implements Container {
  ContainerId id;
  NodeId nodeId;
  WorkerResource workerResource;

  public WorkerResource getWorkerResource() {
    return workerResource;
  }

  public void setWorkerResource(WorkerResource workerResource) {
    this.workerResource = workerResource;
  }

  @Override
  public ContainerId getId() {
    return id;
  }

  @Override
  public void setId(ContainerId id) {
    this.id = id;
  }

  @Override
  public NodeId getNodeId() {
    return nodeId;
  }

  @Override
  public void setNodeId(NodeId nodeId) {
    this.nodeId = nodeId;
  }

  @Override
  public String getNodeHttpAddress() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void setNodeHttpAddress(String nodeHttpAddress) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Resource getResource() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void setResource(Resource resource) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Priority getPriority() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void setPriority(Priority priority) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ContainerState getState() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void setState(ContainerState state) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ContainerToken getContainerToken() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void setContainerToken(ContainerToken containerToken) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ContainerStatus getContainerStatus() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void setContainerStatus(ContainerStatus containerStatus) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int compareTo(Container container) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
