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

package org.apache.tajo.engine.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.engine.planner.FromTable;
import java.util.Map;
import java.util.HashMap;
import java.util.Stack;

//camelia --
public class OuterJoinUtil{

  private static OuterJoinUtil oju;
  // allTables has form  :   <blocName , HashMap <tableName, objTableOuterJoined>>
  public Map<String, HashMap<String, TableOuterJoined>>  allTables; 
  private static final Log LOG = LogFactory.getLog(OuterJoinUtil.class);

  private OuterJoinUtil(){
    allTables = new HashMap<String,HashMap<String,TableOuterJoined>>();
  }

  public static OuterJoinUtil getOuterJoinUtil(){
    if( oju == null) {
       oju = new OuterJoinUtil();
    }
    return oju;
  }

  public void printAllTables(String blockName){
     HashMap<String,TableOuterJoined> tablesInBlock = oju.allTables.get(blockName);
     for (String key : tablesInBlock.keySet()){        
           LOG.info("[QB "+ blockName + " ]" + tablesInBlock.get(key).toString() + "\n");
     }
  }

  public void printAllTables(){
     for (String key : this.allTables.keySet()){
        HashMap<String,TableOuterJoined> tablesInBlock = oju.allTables.get(key);
        for (String key2 : tablesInBlock.keySet()){        
           LOG.info("[QB "+ key + " ]" + tablesInBlock.get(key2).toString() + "\n");
        }
     }
  }

  // puts in the hashmap corresponding to blockName a new pair <tableName, new TableOuterJoined(...)>
  public void putTheTable(String blockName, FromTable ft){
     TableOuterJoined toj = new TableOuterJoined(ft);
     if (this.oju.allTables.containsKey(blockName) == false){
        this.oju.allTables.put(blockName, new HashMap<String,TableOuterJoined>());
     }
     this.oju.allTables.get(blockName).put(toj.theTable.getTableName(), toj);
  }

  // gets the TableOuterJoined of a given tableName in a given blockName
  public TableOuterJoined getTheTable(String blockName, String tableName) {
     return this.oju.allTables.get(blockName).get(tableName);
  }


   
  public static class TableOuterJoined{
     public FromTable theTable;
     public boolean isNullSupplying;  //if it is a null supplying table in any join
     public int countLeft;  //number of left outer joins that it participates in
     public int countRight; //number of right outer joins that it participates in
     public int countFull;  //number of full outer joins that it participates in
     public int countInner;  //number of inner joins that it participates in
     public int countNullSupplying; //number of times it acts as a null supplying table in left or right outer joins
     public boolean isNullRestricted; //whether from some inner join condition or null-intoleratnt selection (WHERE clause)
     public int depthRestricted; //the depth under which it is restricted
  
     public TableOuterJoined(FromTable ft){
         this.theTable = ft;
         this.countLeft = 0;
         this.countRight = 0;
         this.countFull = 0;
         this.countInner = 0;
         this.isNullSupplying = false;
         this.countNullSupplying = 0;
         this.isNullRestricted = false;
         this.depthRestricted = -1;
     }

     public String toString(){
         String s =" " + this.theTable.getTableName() + "  " + this.countLeft + " " + this.countRight + " " + this.countFull + " " + this.countInner + " ";
         s += (this.isNullSupplying)? "yes":"no";
         s += " " + this.countNullSupplying;
         s += (this.isNullRestricted)?"yes":"no";
         s += " " + this.depthRestricted;
         return s;
     }

  } //class
  
}
//-- camelia
