package com.wrmsr.presto.hardcoded;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.wrmsr.presto.util.ImmutableCollectors;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Locale.ENGLISH;

public class HardcodedMetadataPopulator
{
    private final ConnectorManager connectorManager;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final List<PlanOptimizer> planOptimizers;
    private final boolean experimentalSyntaxEnabled;


    public HardcodedMetadataPopulator(ConnectorManager connectorManager, Metadata metadata, SqlParser sqlParser, List<PlanOptimizer> planOptimizers, FeaturesConfig featuresConfig)
    {
        this.connectorManager = checkNotNull(connectorManager);
        this.metadata = checkNotNull(metadata);
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

    public void buildView(Session session, String sql)
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

        try {
            List<ViewDefinition.ViewColumn> columns = analysis.getOutputDescriptor()
                    .getVisibleFields().stream()
                    .map(field -> new ViewDefinition.ViewColumn(field.getName().get(), field.getType()))
                    .collect(toImmutableList());

            new ViewDefinition(sql, session.getCatalog(), session.getSchema(), columns);
        }
        catch (SemanticException e) {
            // e.getCode() == SemanticErrorCode.MISSING_TABLE
        }
    }

    public void run()
    {
        List<HardcodedConnector> conns = connectorManager.getConnectors().values().stream()
                .filter(c -> c instanceof HardcodedConnector)
                .map(c -> (HardcodedConnector) c)
                .collect(ImmutableCollectors.toImmutableList());
        System.out.println(conns);

        Session session = Session.builder()
                .setUser("system")
                .setSource("system")
                .setCatalog("system")
                .setSchema("system")
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();

        for (HardcodedConnector c : conns) {
            HardcodedContents contents = c.getHardcodedContents();
            for (String sql : contents.getViews().values()) {
                buildView(session, sql);
            }
        }
    }
}
