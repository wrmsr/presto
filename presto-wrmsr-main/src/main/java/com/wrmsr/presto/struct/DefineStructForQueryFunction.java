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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static java.util.Locale.ENGLISH;

public class DefineStructForQueryFunction
        extends SqlScalarFunction
{
    private static final String FUNCTION_NAME = "define_struct_for_query";
    private static final Signature SIGNATURE = new Signature(
            FUNCTION_NAME, SCALAR, ImmutableList.of(comparableTypeParameter("varchar")), ImmutableList.of(), parseTypeSignature(StandardTypes.VARCHAR), ImmutableList.of(parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)), false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(DefineStructForQueryFunction.class, "defineStructForQuery", DefineStructForQueryFunction.class, ConnectorSession.class, Slice.class, Slice.class);

    private final SqlParser sqlParser;
    private final List<PlanOptimizer> planOptimizers;
    private final boolean experimentalSyntaxEnabled;
    private final StructManager structManager;
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public DefineStructForQueryFunction(
            StructManager structManager,
            SqlParser sqlParser,
            List<PlanOptimizer> planOptimizers,
            FeaturesConfig featuresConfig,
            Metadata metadata,
            AccessControl accessControl)
    {
        super(new Signature(FUNCTION_NAME, SCALAR, SIGNATURE.getTypeVariableConstraints(), SIGNATURE.getLongVariableConstraints(), parseTypeSignature("varchar"), ImmutableList.of(parseTypeSignature("varchar"), parseTypeSignature("varchar")), false));
        this.structManager = structManager;
        this.sqlParser = checkNotNull(sqlParser);
        this.planOptimizers = checkNotNull(planOptimizers);
        checkNotNull(featuresConfig, "featuresConfig is null");
        this.experimentalSyntaxEnabled = featuresConfig.isExperimentalSyntaxEnabled();
        this.metadata = metadata;
        this.accessControl = accessControl;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return "defines a new struct type for the results of a given query";
    }

    public Analysis analyzeStatement(Statement statement, Session session)
    {
        QueryExplainer explainer = new QueryExplainer(planOptimizers, metadata, accessControl, sqlParser, ImmutableMap.of(), experimentalSyntaxEnabled);
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.of(explainer), experimentalSyntaxEnabled);
        return analyzer.analyze(statement);
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 2);
//        checkArgument(types.size() == 1);

        return new ScalarFunctionImplementation(false, ImmutableList.of(false, false), METHOD_HANDLE.bindTo(this), isDeterministic());
        /*
        return new FunctionInfo(
                new Signature(
                        "define_struct_for_query",
                        StandardTypes.VARCHAR,
                        ImmutableList.of(StandardTypes.VARCHAR, StandardTypes.VARCHAR)),
                getDescription(),
                isHidden(),
                ,
                isDeterministic(),
                true,
                ImmutableList.of(false, false));
        */
    }

    @Nullable
    public RowType buildRowType(String name, String sql)
    {
        Session.SessionBuilder builder = Session.builder(new SessionPropertyManager())
                .setSource("system")
                .setCatalog("default")
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setSchema("default");
        Session session = builder.build();
        return buildRowType(session, name, sql);
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
        RelationType tupleDescriptor;

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

        return new RowType(parameterizedTypeName(name), fieldTypes, fieldNames);
    }

    public static Slice defineStructForQuery(DefineStructForQueryFunction self, ConnectorSession session, Slice name, Slice query)
    {
        // FIXME ConnectorSession
        RowType rowType = self.buildRowType(name.toStringUtf8(), query.toStringUtf8());
        self.structManager.registerStruct(rowType);
        return name;
    }
}
