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
package com.wrmsr.presto.functions;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;

import java.util.List;

public class ExtensionFunctionFactory
        implements com.facebook.presto.metadata.FunctionFactory
{
    private final TypeManager typeManager;
    private final FunctionRegistry functionRegistry;
    private final StructManager structManager;
    private final SqlParser sqlParser;
    private final List<PlanOptimizer> planOptimizers;
    private final FeaturesConfig featuresConfig;
    private final Metadata metadata;

    public ExtensionFunctionFactory(TypeManager typeManager, FunctionRegistry functionRegistry, StructManager structManager, SqlParser sqlParser, List<PlanOptimizer> planOptimizers, FeaturesConfig featuresConfig, Metadata metadata)
    {
        this.typeManager = typeManager;
        this.functionRegistry = functionRegistry;
        this.structManager = structManager;
        this.sqlParser = sqlParser;
        this.planOptimizers = planOptimizers;
        this.featuresConfig = featuresConfig;
        this.metadata = metadata;
    }

    @Override
    public List<ParametricFunction> listFunctions()
    {
        return new FunctionListBuilder(typeManager)
                .scalar(CompressionFunctions.class)
                .function(new SerializeFunction(functionRegistry, structManager))
                .function(new DefineStructFunction(structManager))
                .function(new DefineStructForQueryFunction(structManager, sqlParser, planOptimizers, featuresConfig, metadata))
                //.function(new PropertiesFunction(typeManager))
                //.function(new ConnectFunction())
                .function(Hash.HASH)
                .getFunctions();
    }
}

