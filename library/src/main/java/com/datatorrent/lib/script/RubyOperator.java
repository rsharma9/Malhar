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

package com.datatorrent.lib.script;

import com.datatorrent.api.Context.OperatorContext;
import java.util.Map;
import org.jruby.embed.LocalVariableBehavior;
import org.jruby.embed.ScriptingContainer;
import org.jruby.javasupport.JavaEmbedUtils.EvalUnit;

/**
 * Operator to execute ruby script on tuples
 *
 */
public class RubyOperator extends ScriptOperator {

	public enum Type
	{
		EVAL, INVOKE
	};

	protected Type type = Type.EVAL;
	protected Object evalResult;
	private transient ScriptingContainer sc = null;
	private transient EvalUnit unit;

	public RubyOperator(){
		sc = new ScriptingContainer(LocalVariableBehavior.PERSISTENT);
	}

	@Override
	public void setup(OperatorContext context)
	{
		for (String s : setupScripts) {
			unit = sc.parse(s);
		}
	}

	public void setEval(String script)
	{
		this.type = Type.EVAL;
		this.script = script;
	}

	public void setInvoke(String functionName)
	{
		this.type = Type.INVOKE;
		this.script = functionName;
	}

	@Override
	public void process(Map<String, Object> tuple) {

		try{
			if(type == Type.EVAL){
				for (Map.Entry<String, Object> entry : tuple.entrySet()) {
					sc.put(entry.getKey(), entry.getValue());
				}
				evalResult = unit.run();
			}
			if(type == Type.INVOKE){
				Object[] args = new Object[tuple.size()];
				int index = 0;
				for (Map.Entry<String, Object> entry : tuple.entrySet()) {
					args[index++] = entry.getValue();
				}
				unit.run();
				evalResult = sc.callMethod(evalResult, script, args);
			}
			if (isPassThru && result.isConnected()) {
				result.emit(evalResult);
			}
			if (isPassThru && outBindings.isConnected()) {
				outBindings.emit(getBindings());
			}
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void endWindow()
	{
		if (!isPassThru) {
			result.emit(evalResult);
			outBindings.emit(getBindings());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> getBindings() {
		
		return sc.getVarMap().getMap();
	}
}
