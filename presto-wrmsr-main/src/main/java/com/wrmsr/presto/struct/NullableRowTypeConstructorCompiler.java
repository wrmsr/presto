package com.wrmsr.presto.struct;

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.util.List;

import static com.facebook.presto.byteCode.Parameter.arg;

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
    protected void writeBoolean(ByteCodeBlock body, Variable blockBuilder, Variable arg, int i)
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
    protected void writeLong(ByteCodeBlock body, Variable blockBuilder, Variable arg, int i)
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
    protected void writeDouble(ByteCodeBlock body, Variable blockBuilder, Variable arg, int i)
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
