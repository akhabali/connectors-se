package org.talend.components.jdbc.output.statement.operations.snowflake;

import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.Reject;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.statement.operations.Insert;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.time.LocalDateTime.now;
import static org.talend.components.jdbc.output.statement.operations.snowflake.SnowflakeCopy.upload;

public class SnowflakeInsert extends Insert {

    public SnowflakeInsert(Platform platform, OutputConfig configuration, I18nMessage i18n,
            JdbcService.JdbcDatasource dataSource) {
        super(platform, configuration, i18n, dataSource);
    }

    @Override
    public List<Reject> execute(List<Record> records) throws SQLException {
        buildQuery(records);
        final List<Reject> rejects = new ArrayList<>();
        try (final Connection connection = getDataSource().getConnection()) {
            final String tableName = getConfiguration().getDataset().getTableName();
            final String suffix = now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            final String tmpTableName = "temp_" + tableName + "_" + suffix;
            final String fqTableName = namespace(connection) + "." + getPlatform().identifier(tableName);
            final String fqTmpTableName = namespace(connection) + "." + getPlatform().identifier(tmpTableName);
            final String fqStageName = namespace(connection) + ".%" + getPlatform().identifier(tmpTableName);
            rejects.addAll(upload(connection, records, fqStageName, fqTableName, fqTmpTableName));
            try (final Statement statement = connection.createStatement()) {
                statement.execute("insert into " + fqTableName + getQueryParams().values().stream()
                        .map(e -> getPlatform().identifier(e.getName())).collect(Collectors.joining(",", "(", ")"))
                        + " select * from " + fqTmpTableName);
            }
            connection.commit();
        }
        return rejects;
    }

}
