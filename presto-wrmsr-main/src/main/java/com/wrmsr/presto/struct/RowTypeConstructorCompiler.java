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

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.List;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.google.common.base.Preconditions.checkState;

public class RowTypeConstructorCompiler
{
    protected List<Parameter> createParameters(List<RowType.RowField> fieldTypes)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.size(); i++) {
            RowType.RowField fieldType = fieldTypes.get(i);
            parameters.add(arg("arg" + i, fieldType.getType().getJavaType()));
        }
        return parameters.build();
    }

    protected void annotateParameters(List<RowType.RowField> fieldTypes, MethodDefinition methodDefinition)
    {
        for (int i = 0; i < fieldTypes.size(); i++) {
            methodDefinition.declareParameterAnnotation(SqlType.class, i).setValue("value", fieldTypes.get(i).getType().toString());
        }
    }

    protected void writeBoolean(ByteCodeBlock body, Variable blockBuilder, Variable arg, int i)
    {
        LabelNode isFalse = new LabelNode("isFalse" + i);
        LabelNode done = new LabelNode("done" + i);
        body
                .getVariable(blockBuilder)
                .getVariable(arg)
                .ifFalseGoto(isFalse)
                .push(1)
                .gotoLabel(done)
                .visitLabel(isFalse)
                .push(0)
                .visitLabel(done)
                .invokeInterface(BlockBuilder.class, "writeByte", BlockBuilder.class, int.class)
                .pop()

                .getVariable(blockBuilder)
                .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                .pop();
    }

    protected void writeLong(ByteCodeBlock body, Variable blockBuilder, Variable arg, int i)
    {
        body
                .getVariable(blockBuilder)
                .getVariable(arg)
                .invokeInterface(BlockBuilder.class, "writeLong", BlockBuilder.class, long.class)
                .pop()

                .getVariable(blockBuilder)
                .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                .pop();
    }

    protected void writeDouble(ByteCodeBlock body, Variable blockBuilder, Variable arg, int i)
    {
        body
                .getVariable(blockBuilder)
                .getVariable(arg)
                .invokeInterface(BlockBuilder.class, "writeDouble", BlockBuilder.class, double.class)
                .pop()

                .getVariable(blockBuilder)
                .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                .pop();
    }

    protected void writeSlice(ByteCodeBlock body, Variable blockBuilder, Variable arg, int i)
    {
        LabelNode isNull = new LabelNode("isNull" + i);
        LabelNode done = new LabelNode("done" + i);
        body
                .getVariable(arg)
                .ifNullGoto(isNull)
                .getVariable(blockBuilder)

                .getVariable(arg)
                .push(0)
                .getVariable(arg)
                .invokeVirtual(Slice.class, "length", int.class)
                .invokeInterface(BlockBuilder.class, "writeBytes", BlockBuilder.class, Slice.class, int.class, int.class)
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

    protected void writeObject(ByteCodeBlock body, Variable blockBuilder, Variable arg, int i)
    {
        LabelNode isNull = new LabelNode("isNull" + i);
        LabelNode done = new LabelNode("done" + i);
        body
                .getVariable(arg)
                .ifNullGoto(isNull)
                .getVariable(blockBuilder)

                .getVariable(arg)
                .push(0)
                .getVariable(arg)
                .invokeVirtual(Block.class, "length", int.class)
                .invokeInterface(BlockBuilder.class, "writeBytes", BlockBuilder.class, Block.class, int.class, int.class)
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

    public Class<?> run(RowType rowType)
    {
        return run(rowType, rowType.getTypeSignature().getBase());
    }

    public Class<?> run(RowType rowType, String name)
    {
        // TODO foo_array
        List<RowType.RowField> fieldTypes = rowType.getFields();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(rowType.getDisplayName() + "_new"),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        List<Parameter> parameters = createParameters(fieldTypes);

        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC, STATIC), name, type(Block.class), parameters);
        methodDefinition.declareAnnotation(ScalarFunction.class);
        methodDefinition.declareAnnotation(SqlType.class).setValue("value", rowType.getTypeSignature().toString());
        annotateParameters(fieldTypes, methodDefinition);

        Scope scope = methodDefinition.getScope();
        CallSiteBinder binder = new CallSiteBinder();
        ByteCodeBlock body = methodDefinition.getBody();

        Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder");

        body
                .newObject(type(VariableWidthBlockBuilder.class, BlockBuilderStatus.class))
                .dup()
                .dup()
                .newObject(BlockBuilderStatus.class)
                .dup()
                .invokeConstructor(BlockBuilderStatus.class)
                .invokeConstructor(VariableWidthBlockBuilder.class, BlockBuilderStatus.class)
                .putVariable(blockBuilder);

        // FIXME: reuse returned blockBuilder

        for (int i = 0; i < fieldTypes.size(); i++) {
            Variable arg = scope.getVariable("arg" + i);
            Class<?> javaType = fieldTypes.get(i).getType().getJavaType();

            if (javaType == boolean.class) {
                writeBoolean(body, blockBuilder, arg, i);
            }
            else if (javaType == long.class) {
                writeLong(body, blockBuilder, arg, i);
            }
            else if (javaType == double.class) {
                writeDouble(body, blockBuilder, arg, i);
            }
            else if (javaType == Slice.class) {
                writeSlice(body, blockBuilder, arg, i);
            }
            else if (javaType == Block.class) {
                writeObject(body, blockBuilder, arg, i);
            }
            else {
                throw new IllegalArgumentException("bad value: " + javaType);
            }
        }

        body
                .getVariable(blockBuilder)
                .invokeInterface(BlockBuilder.class, "build", Block.class)
                .retObject();

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(RowTypeConstructorCompiler.class.getClassLoader()));
    }

    public static Slice blockBuilderToSlice(BlockBuilder blockBuilder)
    {
        return blockToSlice(blockBuilder.build());
    }

    public static Slice blockToSlice(Block block)
    {
        BlockEncoding blockEncoding = new VariableWidthBlockEncoding();

        int estimatedSize = blockEncoding.getEstimatedSize(block);
        Slice outputSlice = Slices.allocate(estimatedSize);
        SliceOutput sliceOutput = outputSlice.getOutput();

        blockEncoding.writeBlock(sliceOutput, block);
        checkState(sliceOutput.size() == estimatedSize);

        return outputSlice;
    }
}
