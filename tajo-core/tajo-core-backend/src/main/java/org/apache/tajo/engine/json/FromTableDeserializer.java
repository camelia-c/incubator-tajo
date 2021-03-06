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

/**
 * 
 */
package org.apache.tajo.engine.json;

import com.google.gson.*;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableDescImpl;
import org.apache.tajo.engine.planner.FromTable;
import org.apache.tajo.storage.Fragment;

import java.lang.reflect.Type;

public class FromTableDeserializer implements JsonDeserializer<FromTable> {

	@Override
	public FromTable deserialize(JsonElement json, Type type,
			JsonDeserializationContext ctx) throws JsonParseException {
		Gson gson = CoreGsonHelper.getInstance();
		JsonObject fromTableObj = json.getAsJsonObject();
		boolean isFragment = fromTableObj.get("isFragment").getAsBoolean();
		TableDesc desc;
		if (isFragment) {
			desc = gson.fromJson(fromTableObj.get("desc"), Fragment.class);
		} else {
			desc = gson.fromJson(fromTableObj.get("desc"), TableDescImpl.class);
		}
		
		return new FromTable(desc, fromTableObj.get("alias").getAsString());
	}

}
