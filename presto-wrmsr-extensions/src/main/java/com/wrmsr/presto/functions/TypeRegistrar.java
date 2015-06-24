package com.wrmsr.presto.functions;

import com.facebook.presto.Session;
import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.util.Box;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;

public class TypeRegistrar
{
    private final ConnectorManager connectorManager;
    private final TypeRegistry typeRegistry;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final List<PlanOptimizer> planOptimizers;
    private final boolean experimentalSyntaxEnabled;

    public TypeRegistrar(ConnectorManager connectorManager, TypeRegistry typeRegistry, Metadata metadata, SqlParser sqlParser, List<PlanOptimizer> planOptimizers, FeaturesConfig featuresConfig)
    {
        this.connectorManager = checkNotNull(connectorManager);
        this.typeRegistry = typeRegistry;
        this.metadata = checkNotNull(metadata);
        this.sqlParser = checkNotNull(sqlParser);
        this.planOptimizers = checkNotNull(planOptimizers);
        checkNotNull(featuresConfig, "featuresConfig is null");
        this.experimentalSyntaxEnabled = featuresConfig.isExperimentalSyntaxEnabled();
    }

    public Analysis analyzeStatement(Statement statement, Session session)
    {
        QueryExplainer explainer = new QueryExplainer(session, planOptimizers, metadata, sqlParser, experimentalSyntaxEnabled);
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, Optional.of(explainer), experimentalSyntaxEnabled);
        return analyzer.analyze(statement);
    }

    @Nullable
    public RowType buildRowType(Session session, String name, String sql)
    {
        checkArgument(name.toLowerCase().equals(name));

        // verify round-trip
        Statement statement;
        try {
            statement = sqlParser.createStatement(sql);
        }
        catch (ParsingException e) {
            throw new PrestoException(INTERNAL_ERROR, "Formatted query does not parse: " + sql);
        }

        Analysis analysis = analyzeStatement(statement, session);
        TupleDescriptor tupleDescriptor;

        try {
            tupleDescriptor = analysis.getOutputDescriptor();
        }
        catch (SemanticException e) {
            if (e.getCode() == SemanticErrorCode.MISSING_TABLE) {
                return null;
            }
            else {
                throw e;
            }
        }

        Collection<Field> visibleFields = tupleDescriptor.getVisibleFields();
        List<Type> fieldTypes = visibleFields.stream().map(f -> f.getType()).collect(toImmutableList());
        List<Optional<String>> fieldNameOptions = visibleFields.stream().map(f -> f.getName()).collect(toImmutableList());
        long numNamed = fieldNameOptions.stream().filter(o -> o.isPresent()).count();
        Optional<List<String>> fieldNames;
        if (numNamed == (long) fieldNameOptions.size()) {
            fieldNames = Optional.of(fieldNameOptions.stream().map(o -> o.get()).collect(toImmutableList()));
        }
        else if (numNamed == 0) {
            fieldNames = Optional.empty();
        }
        else {
            throw new RuntimeException(String.format("All fields must be named or no fields must be named for type: %s -> %s", name, sql));
        }

        RowType rt = new RowType(parameterizedTypeName(name), fieldTypes, fieldNames);
        return rt;
    }

    public static class RowTypeConstructorCompiler
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

        protected void writeBoolean(com.facebook.presto.byteCode.Block body, Variable blockBuilder, Variable arg, int i)
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

        protected void writeLong(com.facebook.presto.byteCode.Block body, Variable blockBuilder, Variable arg, int i)
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

        protected void writeDouble(com.facebook.presto.byteCode.Block body, Variable blockBuilder, Variable arg, int i)
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

        protected void writeSlice(com.facebook.presto.byteCode.Block body, Variable blockBuilder, Variable arg, int i)
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

            MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC, STATIC), name, type(Slice.class), parameters);
            methodDefinition.declareAnnotation(ScalarFunction.class);
            methodDefinition.declareAnnotation(SqlType.class).setValue("value", rowType.getTypeSignature().toString());
            annotateParameters(fieldTypes, methodDefinition);

            Scope scope = methodDefinition.getScope();
            CallSiteBinder binder = new CallSiteBinder();
            com.facebook.presto.byteCode.Block body = methodDefinition.getBody();

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
                else {
                    throw new IllegalArgumentException("bad value: " + javaType);
                }
            }

            body
                    .getVariable(blockBuilder)
                    .invokeStatic(RowTypeConstructorCompiler.class, "blockBuilderToSlice", Slice.class, BlockBuilder.class)
                    .retObject();

            return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(RowTypeConstructorCompiler.class.getClassLoader()));
        }

        public static Slice blockBuilderToSlice(BlockBuilder blockBuilder)
        {
            return blockToSlice(blockBuilder.build());
        }

        public static Slice blockToSlice(com.facebook.presto.spi.block.Block block)
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

    public static class NullableRowTypeConstructorCompiler extends RowTypeConstructorCompiler
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
                    // nop
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
        protected void writeBoolean(Block body, Variable blockBuilder, Variable arg, int i)
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
        protected void writeLong(Block body, Variable blockBuilder, Variable arg, int i)
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
        protected void writeDouble(Block body, Variable blockBuilder, Variable arg, int i)
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

    public static Class<?> generateBox(String name)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(name + "$box"),
                type(Box.class, Slice.class));

        MethodDefinition methodDefinition = definition.declareConstructor(a(PUBLIC), ImmutableList.of(arg("slice", Slice.class)));
        methodDefinition.getBody()
                .getVariable(methodDefinition.getThis())
                .getVariable(methodDefinition.getScope().getVariable("slice"))
                .invokeConstructor(Box.class, Object.class)
                .ret();

        return defineClass(definition, Object.class, new CallSiteBinder().getBindings(), new DynamicClassLoader(TypeRegistrar.class.getClassLoader()));
    }

    public void run()
    {
        Session.SessionBuilder builder = Session.builder()
                .setUser("system")
                .setSource("system")
                .setCatalog("yelp")
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setSchema("yelp");
        Session session = builder.build();

        RowType rowType = buildRowType(session, "thing", "select 1, 'hi', cast(null as bigint), cast(null as varbinary)");

        typeRegistry.addType(rowType);
        metadata.addFunctions(
                new FunctionListBuilder(typeRegistry)
                        .scalar(new RowTypeConstructorCompiler().run(rowType, rowType.getTypeSignature().getBase() + "_strict"))
                        .scalar(new NullableRowTypeConstructorCompiler().run(rowType, rowType.getTypeSignature().getBase()))
                        .getFunctions());
    }
}
