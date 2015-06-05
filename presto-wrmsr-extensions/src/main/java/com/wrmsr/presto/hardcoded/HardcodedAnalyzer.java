package com.wrmsr.presto.hardcoded;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.google.inject.Inject;
import com.wrmsr.presto.util.Configs;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkNotNull;

public class HardcodedAnalyzer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final List<PlanOptimizer> planOptimizers;
    private final boolean experimentalSyntaxEnabled;

    @Inject
    public HardcodedAnalyzer(Metadata metadata, SqlParser sqlParser, List<PlanOptimizer> planOptimizers, FeaturesConfig featuresConfig)
    {
        this.metadata = metadata;
        this.sqlParser = checkNotNull(sqlParser);
        this.planOptimizers = checkNotNull(planOptimizers);
        checkNotNull(featuresConfig, "featuresConfig is null");
        this.experimentalSyntaxEnabled = featuresConfig.isExperimentalSyntaxEnabled();
    }

    public Analysis analyzeStatement(Statement statement, Session session, Metadata metadata)
    {
        QueryExplainer explainer = new QueryExplainer(session, planOptimizers, metadata, sqlParser, experimentalSyntaxEnabled);
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, Optional.of(explainer), experimentalSyntaxEnabled);
        return analyzer.analyze(statement);
    }

    public void doShit(Session session, String sql) throws Throwable
    {
        // verify round-trip
        Statement statement;
        try {
            statement = sqlParser.createStatement(sql);
        }
        catch (ParsingException e) {
            throw new PrestoException(INTERNAL_ERROR, "Formatted query does not parse: " + sql);
        }

        // QualifiedTableName name = createQualifiedTableName(session, statement.getName());

        Analysis analysis = analyzeStatement(statement, session, metadata);

        List<ViewDefinition.ViewColumn> columns = analysis.getOutputDescriptor()
                .getVisibleFields().stream()
                .map(field -> new ViewDefinition.ViewColumn(field.getName().get(), field.getType()))
                .collect(toImmutableList());

        new ViewDefinition(sql, session.getCatalog(), session.getSchema(), columns);
    }
}
