package com.wrmsr.presto.functions;

import com.facebook.presto.Session;
import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
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
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;

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

    public TypeRegistrar(ConnectorManager connectorManager,TypeRegistry typeRegistry, Metadata metadata, SqlParser sqlParser, List<PlanOptimizer> planOptimizers, FeaturesConfig featuresConfig)
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
    public Type buildType(Session session, String name, String sql)
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

        Type rt = new RowType(parameterizedTypeName(name), fieldTypes, fieldNames);
        return rt;
    }

    public void generateConstructor(RowType rowType)
    {
        List<RowType.RowField> fieldTypes = rowType.getFields();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(rowType.getDisplayName() + "_new"),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.size(); i++) {
            RowType.RowField fieldType = fieldTypes.get(i);
            parameters.add(arg("arg" + i, fieldType.getType().getJavaType()));
        }

        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC, STATIC), "_new", type(Slice.class), parameters.build());
        Scope scope = methodDefinition.getScope();

        Variable typeVariable = scope.declareVariable(Type.class, "typeVariable");
        Variable rangeTypeVariable = scope.declareVariable(Type.class, "rangeTypeVariable");
        CallSiteBinder binder = new CallSiteBinder();
        Block body = methodDefinition.getBody();

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
        Type thing = buildType(session, "thing", "select 1, 'hi', cast(null as bigint), cast(null as varbinary)");
        typeRegistry.addType(thing);
    }
}
