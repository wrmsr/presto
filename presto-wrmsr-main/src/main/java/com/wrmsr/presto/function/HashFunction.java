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
// https://github.com/shubham166/presto/tree/Hash_Function
//package com.wrmsr.presto.function;
//
//import com.facebook.presto.bytecode.BytecodeBlock;
//import com.facebook.presto.bytecode.ClassDefinition;
//import com.facebook.presto.bytecode.CompilerUtils;
//import com.facebook.presto.bytecode.DynamicClassLoader;
//import com.facebook.presto.bytecode.MethodDefinition;
//import com.facebook.presto.bytecode.Parameter;
//import com.facebook.presto.bytecode.Scope;
//import com.facebook.presto.bytecode.Variable;
//import com.facebook.presto.metadata.BoundVariables;
//import com.facebook.presto.metadata.FunctionRegistry;
//import com.facebook.presto.metadata.SqlScalarFunction;
//import com.facebook.presto.metadata.TypeVariableConstraint;
//import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
//import com.facebook.presto.spi.PrestoException;
//import com.facebook.presto.spi.block.BlockBuilder;
//import com.facebook.presto.spi.block.BlockBuilderStatus;
//import com.facebook.presto.spi.type.StandardTypes;
//import com.facebook.presto.spi.type.Type;
//import com.facebook.presto.spi.type.TypeManager;
//import com.facebook.presto.sql.gen.CallSiteBinder;
//import com.facebook.presto.type.BigintOperators;
//import com.facebook.presto.util.ImmutableCollectors;
//import com.google.common.base.Joiner;
//import com.google.common.collect.ImmutableList;
//import io.airlift.slice.Slice;
//
//import java.util.List;
//import java.util.Map;
//import java.util.stream.IntStream;
//
//import static com.facebook.presto.bytecode.Access.FINAL;
//import static com.facebook.presto.bytecode.Access.PRIVATE;
//import static com.facebook.presto.bytecode.Access.PUBLIC;
//import static com.facebook.presto.bytecode.Access.STATIC;
//import static com.facebook.presto.bytecode.Access.a;
//import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
//import static com.facebook.presto.bytecode.Parameter.arg;
//import static com.facebook.presto.bytecode.ParameterizedType.type;
//import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
//import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
//import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
//import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
//import static java.lang.String.format;
//
//public final class HashFunction
//        extends SqlScalarFunction
//{
//    public static final String NAME = "hash";
//
//    public HashFunction(int arity)
//    {
//        super(
//                NAME,
//                IntStream.range(0, arity).boxed().map(n -> new TypeVariableConstraint("T" + n.toString(), false, false, null)).collect(toImmutableList()),
//                ImmutableList.of(),
//                "bigint",
//                ImmutableList.<String>builder()
//                        .addAll(IntStream.range(0, arity).boxed().map(n -> "T" + n.toString()).collect(toImmutableList()))
//                        .build());
//    }
//
//    @Override
//    public boolean isHidden()
//    {
//        return false;
//    }
//
//    @Override
//    public boolean isDeterministic()
//    {
//        return true;
//    }
//
//    @Override
//    public String getDescription()
//    {
//        return "get the hash value within the given range for variable no. of arguments of any type";
//    }
//
//    public static void checkNotNaN(double value)
//    {
//        if (Double.isNaN(value)) {
//            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid argument to hash(): NaN");
//        }
//    }
//
//    /*
//    @Override
//    public SqlScalarFunction specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
//    {
//        Type rangeType = types.get("bigint");
//        Type type = types.get("E");
//
//        // the argument need not be orderable, so no orderable check
//
//        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
//        builder.add(rangeType.getJavaType());
//        for (int i = 1; i < arity; i++) {
//            builder.add(type.getJavaType());
//        }
//
//        ImmutableList<Class<?>> stackTypes = builder.build();
//        Class<?> clazz = generateHash(stackTypes, rangeType, type);
//        MethodHandle methodHandle = methodHandle(clazz, "hash", stackTypes.toArray(new Class<?>[stackTypes.size()]));
//        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(stackTypes.size(), false));
//
//        ImmutableList.Builder<TypeSignature> typesigBuilder = ImmutableList.builder();
//        typesigBuilder.add(rangeType.getTypeSignature());
//        for (int i = 1; i < arity; i++) {
//            typesigBuilder.add(type.getTypeSignature());
//        }
//        ImmutableList<TypeSignature> typesigList = typesigBuilder.build();
//        Signature specializedSignature = internalFunction(SIGNATURE.getName(), BIGINT.getTypeSignature(), typesigList);
//        return new FunctionInfo(specializedSignature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, nullableParameters);
//    }
//    */
//
//    @Override
//    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
//    {
//        return null;
//    }
//
//    public static Class<?> generateHash(List<Class<?>> nativeContainerTypes, Type rangeType, Type type)
//    {
//        List<String> nativeContainerTypeNames = nativeContainerTypes.stream().map(Class::getSimpleName).collect(ImmutableCollectors.toImmutableList());
//        ClassDefinition definition = new ClassDefinition(
//                a(PUBLIC, FINAL),
//                CompilerUtils.makeClassName(Joiner.on("").join(nativeContainerTypeNames) + "Hash"),
//                type(Object.class));
//
//        definition.declareDefaultConstructor(a(PRIVATE));
//
//        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
//        for (int i = 0; i < nativeContainerTypes.size(); i++) {
//            Class<?> nativeContainerType = nativeContainerTypes.get(i);
//            parameters.add(arg("arg" + i, nativeContainerType));
//        }
//
//        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC, STATIC), "hash", type(nativeContainerTypes.get(0)), parameters.build());
//        Scope scope = methodDefinition.getScope();
//
//        Variable typeVariable = scope.declareVariable(Type.class, "typeVariable");
//        Variable rangeTypeVariable = scope.declareVariable(Type.class, "rangeTypeVariable");
//        CallSiteBinder binder = new CallSiteBinder();
//        BytecodeBlock body = methodDefinition.getBody();
//
//        body.comment("rangeTypeVariable = rangeType")
//                .append(constantType(binder, rangeType))
//                .putVariable(rangeTypeVariable);
//
//        body.comment("typeVariable = type;")
//                .append(constantType(binder, type))
//                .putVariable(typeVariable);
//
//        for (int i = 0; i < nativeContainerTypes.size(); i++) {
//            Class<?> nativeContainerType = nativeContainerTypes.get(i);
//            Variable currentBlock = scope.declareVariable(com.facebook.presto.spi.block.Block.class, "block" + i);
//            Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder" + i);
//            BytecodeBlock buildBlock = new BytecodeBlock()
//                    .comment("blockBuilder%d = typeVariable.createBlockBuilder(new BlockBuilderStatus());", i)
//                    .getVariable(i == 0 ? rangeTypeVariable : typeVariable)
//                    .newObject(BlockBuilderStatus.class)
//                    .dup()
//                    .invokeConstructor(BlockBuilderStatus.class)
//                    .invokeInterface(Type.class, "createBlockBuilder", BlockBuilder.class, BlockBuilderStatus.class)
//                    .putVariable(blockBuilder);
//
//            String writeMethodName;
//            if (nativeContainerType == long.class) {
//                writeMethodName = "writeLong";
//            }
//            else if (nativeContainerType == boolean.class) {
//                writeMethodName = "writeBoolean";
//            }
//            else if (nativeContainerType == double.class) {
//                writeMethodName = "writeDouble";
//            }
//            else if (nativeContainerType == Slice.class) {
//                writeMethodName = "writeSlice";
//            }
//            else {
//                throw new PrestoException(INTERNAL_ERROR, format("Unexpected type %s", nativeContainerType.getName()));
//            }
//
//            if (i > 0 && type.getTypeSignature().getBase().equals(StandardTypes.DOUBLE)) {
//                buildBlock.comment("arg1 != NaN")
//                        .getVariable(scope.getVariable("arg" + i))
//                        .invokeStatic(HashFunction.class, "checkNotNaN", void.class, double.class);
//            }
//
//            BytecodeBlock writeBlock = new BytecodeBlock()
//                    .comment("typeVariable.%s(blockBuilder%d, arg%d);", writeMethodName, i, i)
//                    .getVariable(i == 0 ? rangeTypeVariable : typeVariable)
//                    .getVariable(blockBuilder)
//                    .getVariable(scope.getVariable("arg" + i))
//                    .invokeInterface(Type.class, writeMethodName, void.class, BlockBuilder.class, nativeContainerType);
//
//            buildBlock.append(writeBlock);
//
//            BytecodeBlock storeBlock = new BytecodeBlock()
//                    .comment("block%d = blockBuilder%d.build();", i, i)
//                    .getVariable(blockBuilder)
//                    .invokeInterface(BlockBuilder.class, "build", com.facebook.presto.spi.block.Block.class)
//                    .putVariable(currentBlock);
//            buildBlock.append(storeBlock);
//            body.append(buildBlock);
//        }
//
//        Variable rangeVariable = scope.declareVariable(nativeContainerTypes.get(0), "range");
//        Variable rangeBlockVariable = scope.declareVariable(com.facebook.presto.spi.block.Block.class, "rangeBlock");
//
//        body.comment("range = arg0; rangeBlock = block0;")
//                .getVariable(scope.getVariable("arg0"))
//                .putVariable(rangeVariable)
//                .getVariable(scope.getVariable("block0"))
//                .putVariable(rangeBlockVariable);
//
//        Variable hashValueVariable = scope.declareVariable(nativeContainerTypes.get(0), "hashValue");
//        body.comment("hashValue = 0")
//                .push(0)
//                .intToLong()
//                .putVariable(hashValueVariable);
//
//        Variable currenHashValueVariable = scope.declareVariable(nativeContainerTypes.get(0), "currentHashValue");
//        Variable currentBlockLengthVariable = scope.declareVariable(int.class, "currentLength");
//        for (int i = 1; i < nativeContainerTypes.size(); i++) {
//            BytecodeBlock currentBlockLength = new BytecodeBlock()
//                    .getVariable(scope.getVariable("block" + i))
//                    .push(0)
//                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "getLength", int.class, int.class)
//                    .putVariable(currentBlockLengthVariable);
//
//            BytecodeBlock currentHashValueBlock = new BytecodeBlock()
//                    .getVariable(scope.getVariable("block" + i))
//                    .push(0)
//                    .push(0)
//                    .getVariable(currentBlockLengthVariable)
//                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "hash", int.class, int.class, int.class, int.class)
//                    .intToLong()
//                    .getVariable(scope.getVariable("range"))
//                    .invokeStatic(BigintOperators.class, "modulus", long.class, long.class, nativeContainerTypes.get(0))
//                    .putVariable(currenHashValueVariable);
//
//            BytecodeBlock updateHashValueBlock = new BytecodeBlock()
//                    .getVariable(currenHashValueVariable)
//                    .getVariable(hashValueVariable)
//                    .invokeStatic(BigintOperators.class, "add", long.class, long.class, long.class)
//                    .getVariable(scope.getVariable("range"))
//                    .invokeStatic(BigintOperators.class, "modulus", long.class, long.class, nativeContainerTypes.get(0))
//                    .putVariable(hashValueVariable);
//
//            body.append(currentBlockLength)
//                    .append(currentHashValueBlock)
//                    .append(updateHashValueBlock);
//        }
//        body.comment("return hashValue")
//                .getVariable(hashValueVariable)
//                .getVariable(scope.getVariable("range"))
//                .invokeStatic(BigintOperators.class, "add", long.class, long.class, long.class)
//                .getVariable(scope.getVariable("range"))
//                .invokeStatic(BigintOperators.class, "modulus", long.class, long.class, nativeContainerTypes.get(0))
//                .ret(nativeContainerTypes.get(0));
//
//        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(HashFunction.class.getClassLoader()));
//    }
//}
