/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.struct;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.util.List;

import static com.facebook.presto.bytecode.Parameter.arg;

public class NullableRowTypeConstructorCompiler
        extends RowTypeConstructorCompiler
{
    @Override
    protected List<Parameter> createParameters(List<RowType.RowField> fieldTypes)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.size(); i++) {
            RowType.RowField fieldType = fieldTypes.get(i);
            Class<?> javaType = fieldType.getType().getJavaType();
            if (javaType == boolean.class) {
                javaType = Boolean.class;
            }
            else if (javaType == long.class) {
                javaType = Long.class;
            }
            else if (javaType == double.class) {
                javaType = Double.class;
            }
            else if (javaType == Slice.class) {
                javaType = Slice.class;
            }
            else if (javaType == Block.class) {
                // FIXME
            }
            else {
                throw new IllegalArgumentException("javaType: " + javaType.toString());
            }
            parameters.add(arg("arg" + i, javaType));
        }
        return parameters.build();
    }

    @Override
    protected void annotateParameters(List<RowType.RowField> fieldTypes, MethodDefinition methodDefinition)
    {
        for (int i = 0; i < fieldTypes.size(); i++) {
            methodDefinition.declareParameterAnnotation(Nullable.class, i);
            methodDefinition.declareParameterAnnotation(SqlType.class, i).setValue("value", fieldTypes.get(i).getType().toString());
        }
    }

    @Override
    protected void writeBoolean(BytecodeBlock body, Variable blockBuilder, Variable arg, int i)
    {
        LabelNode isNull = new LabelNode("isNull" + i);
        LabelNode isFalse = new LabelNode("isFalse" + i);
        LabelNode write = new LabelNode("write" + i);
        LabelNode done = new LabelNode("done" + i);
        body
                .getVariable(arg)
                .ifNullGoto(isNull)
                .getVariable(blockBuilder)

                .getVariable(arg)
                .invokeVirtual(Boolean.class, "booleanValue", boolean.class)
                .ifFalseGoto(isFalse)
                .push(1)
                .gotoLabel(write)
                .visitLabel(isFalse)
                .push(0)
                .visitLabel(write)
                .invokeInterface(BlockBuilder.class, "writeByte", BlockBuilder.class, int.class)
                .pop()

                .getVariable(blockBuilder)
                .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                .pop()

                .gotoLabel(done)
                .visitLabel(isNull)
                .getVariable(blockBuilder)
                .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                .pop()
                .visitLabel(done);
    }

    @Override
    protected void writeLong(BytecodeBlock body, Variable blockBuilder, Variable arg, int i)
    {
        LabelNode isNull = new LabelNode("isNull" + i);
        LabelNode done = new LabelNode("done" + i);
        body
                .getVariable(arg)
                .ifNullGoto(isNull)
                .getVariable(blockBuilder)

                .getVariable(arg)
                .invokeVirtual(Long.class, "longValue", long.class)
                .invokeInterface(BlockBuilder.class, "writeLong", BlockBuilder.class, long.class)
                .pop()

                .getVariable(blockBuilder)
                .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                .pop()

                .gotoLabel(done)
                .visitLabel(isNull)
                .getVariable(blockBuilder)
                .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                .pop()
                .visitLabel(done);
    }

    @Override
    protected void writeDouble(BytecodeBlock body, Variable blockBuilder, Variable arg, int i)
    {
        LabelNode isNull = new LabelNode("isNull" + i);
        LabelNode done = new LabelNode("done" + i);
        body
                .getVariable(arg)
                .ifNullGoto(isNull)
                .getVariable(blockBuilder)

                .getVariable(arg)
                .invokeVirtual(Double.class, "doubleValue", double.class)
                .invokeInterface(BlockBuilder.class, "writeDouble", BlockBuilder.class, double.class)
                .pop()

                .getVariable(blockBuilder)
                .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                .pop()

                .gotoLabel(done)
                .visitLabel(isNull)
                .getVariable(blockBuilder)
                .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                .pop()
                .visitLabel(done);
    }
}
