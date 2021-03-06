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

package org.apache.tajo.engine.query;

import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.ipc.TajoWorkerProtocol.Fetch;
import org.apache.tajo.ipc.TajoWorkerProtocol.QueryUnitRequestProto;
import org.apache.tajo.ipc.TajoWorkerProtocol.QueryUnitRequestProtoOrBuilder;
import org.apache.tajo.ipc.protocolrecords.QueryUnitRequest;
import org.apache.tajo.master.QueryMeta;
import org.apache.tajo.storage.Fragment;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class QueryUnitRequestImpl implements QueryUnitRequest {
	
  private QueryUnitAttemptId id;
  private List<Fragment> fragments;
  private String outputTable;
	private boolean isUpdated;
	private boolean clusteredOutput;
	private String serializedData;     // logical node
	private Boolean interQuery;
	private List<Fetch> fetches;
  private Boolean shouldDie;
  private QueryMeta queryMeta;
	
	private QueryUnitRequestProto proto = QueryUnitRequestProto.getDefaultInstance();
	private QueryUnitRequestProto.Builder builder = null;
	private boolean viaProto = false;
	
	public QueryUnitRequestImpl() {
		builder = QueryUnitRequestProto.newBuilder();
		this.id = null;
		this.isUpdated = false;
	}
	
	public QueryUnitRequestImpl(QueryUnitAttemptId id, List<Fragment> fragments,
			String outputTable, boolean clusteredOutput,
			String serializedData, QueryMeta queryMeta) {
		this();
		this.set(id, fragments, outputTable, clusteredOutput, serializedData, queryMeta);
	}
	
	public QueryUnitRequestImpl(QueryUnitRequestProto proto) {
		this.proto = proto;
		viaProto = true;
		id = null;
		isUpdated = false;
	}
	
	public void set(QueryUnitAttemptId id, List<Fragment> fragments,
			String outputTable, boolean clusteredOutput, 
			String serializedData, QueryMeta queryMeta) {
		this.id = id;
		this.fragments = fragments;
		this.outputTable = outputTable;
		this.clusteredOutput = clusteredOutput;
		this.serializedData = serializedData;
		this.isUpdated = true;
    this.queryMeta = queryMeta;
	}

	@Override
	public QueryUnitRequestProto getProto() {
		mergeLocalToProto();
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		return proto;
	}

	@Override
	public QueryUnitAttemptId getId() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (id != null) {
			return this.id;
		}
		if (!p.hasId()) {
			return null;
		}
		this.id = new QueryUnitAttemptId(p.getId());
		return this.id;
	}

	@Override
	public List<Fragment> getFragments() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (fragments != null) {
			return fragments;
		}
		if (fragments == null) {
			fragments = new ArrayList<Fragment>();
		}
		for (int i = 0; i < p.getFragmentsCount(); i++) {
			fragments.add(new Fragment(p.getFragments(i)));
		}
		return this.fragments;
	}

	@Override
	public String getOutputTableId() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (outputTable != null) {
			return this.outputTable;
		}
		if (!p.hasOutputTable()) {
			return null;
		}
		this.outputTable = p.getOutputTable();
		return this.outputTable;
	}

	@Override
	public boolean isClusteredOutput() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (isUpdated) {
			return this.clusteredOutput;
		}
		if (!p.hasClusteredOutput()) {
			return false;
		}
		this.clusteredOutput = p.getClusteredOutput();
		this.isUpdated = true;
		return this.clusteredOutput;
	}

	@Override
	public String getSerializedData() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (this.serializedData != null) {
			return this.serializedData;
		}
		if (!p.hasSerializedData()) {
			return null;
		}
		this.serializedData = p.getSerializedData();
		return this.serializedData;
	}
	
	public boolean isInterQuery() {
	  QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (interQuery != null) {
      return interQuery;
    }
    if (!p.hasInterQuery()) {
      return false;
    }
    this.interQuery = p.getInterQuery();
    return this.interQuery;
	}
	
	public void setInterQuery() {
	  maybeInitBuilder();
	  this.interQuery = true;
	}
	
	public void addFetch(String name, URI uri) {
	  maybeInitBuilder();
	  initFetches();
	  fetches.add(
	  Fetch.newBuilder()
	    .setName(name)
	    .setUrls(uri.toString()).build());
	  
	}

  public QueryMeta getQueryMeta() {
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (queryMeta != null) {
      return queryMeta;
    }
    if (!p.hasQueryMeta()) {
      return null;
    }
    this.queryMeta = new QueryMeta(p.getQueryMeta());
    return this.queryMeta;
  }

  public void setQueryMeta(QueryMeta queryMeta) {
    maybeInitBuilder();
    this.queryMeta = queryMeta;
  }
	
	public List<Fetch> getFetches() {
	  initFetches();    

    return this.fetches;
	}
	
	private void initFetches() {
	  if (this.fetches != null) {
      return;
    }
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.fetches = new ArrayList<Fetch>();
    for(Fetch fetch : p.getFetchesList()) {
      fetches.add(fetch);
    }
	}

  @Override
  public boolean shouldDie() {
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (shouldDie != null) {
      return shouldDie;
    }
    if (!p.hasShouldDie()) {
      return false;
    }
    this.shouldDie = p.getShouldDie();
    return this.shouldDie;
  }

  @Override
  public void setShouldDie() {
    maybeInitBuilder();
    shouldDie = true;
  }

  private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = QueryUnitRequestProto.newBuilder(proto);
		}
		viaProto = true;
	}
	
	private void mergeLocalToBuilder() {
		if (id != null) {
			builder.setId(this.id.getProto());
		}
		if (fragments != null) {
			for (int i = 0; i < fragments.size(); i++) {
				builder.addFragments(fragments.get(i).getProto());
			}
		}
		if (this.outputTable != null) {
			builder.setOutputTable(this.outputTable);
		}
		if (this.isUpdated) {
			builder.setClusteredOutput(this.clusteredOutput);
		}
		if (this.serializedData != null) {
			builder.setSerializedData(this.serializedData);
		}
		if (this.interQuery != null) {
		  builder.setInterQuery(this.interQuery);
		}
		if (this.fetches != null) {
		  builder.addAllFetches(this.fetches);
		}
    if (this.shouldDie != null) {
      builder.setShouldDie(this.shouldDie);
    }
    if (this.queryMeta != null) {
      builder.setQueryMeta(queryMeta.getProto());
    }
	}

	private void mergeLocalToProto() {
		if(viaProto) {
			maybeInitBuilder();
		}
		mergeLocalToBuilder();
		proto = builder.build();
		viaProto = true;
	}
}
