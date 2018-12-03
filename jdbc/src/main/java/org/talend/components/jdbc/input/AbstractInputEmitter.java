package org.talend.components.jdbc.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.InputConfig;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Date;

@Slf4j
public abstract class AbstractInputEmitter implements Serializable {

    private final InputConfig inputConfig;

    private RecordBuilderFactory recordBuilderFactory;

    private final JdbcService jdbcDriversService;

    private final I18nMessage i18n;

    protected Connection connection;

    private Statement statement;

    private ResultSet resultSet;

    private JdbcService.JdbcDatasource dataSource;

    AbstractInputEmitter(final InputConfig inputConfig, final JdbcService jdbcDriversService,
            final RecordBuilderFactory recordBuilderFactory, final I18nMessage i18nMessage) {
        this.inputConfig = inputConfig;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jdbcDriversService = jdbcDriversService;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        if (inputConfig.getDataSet().getQuery() == null || inputConfig.getDataSet().getQuery().trim().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyQuery());
        }
        if (jdbcDriversService.isNotReadOnlySQLQuery(inputConfig.getDataSet().getQuery())) {
            throw new IllegalArgumentException(i18n.errorUnauthorizedQuery());
        }

        try {
            dataSource = jdbcDriversService.createDataSource(inputConfig.getDataSet().getConnection());
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            statement.setFetchSize(inputConfig.getFetchSize());
            resultSet = statement.executeQuery(inputConfig.getDataSet().getQuery());
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Producer
    public Record next() {
        try {
            if (!resultSet.next()) {
                return null;
            }

            final Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder();
            final ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                final String name = metaData.getColumnName(i);
                final int sqlType = metaData.getColumnType(i);
                final String javaType = metaData.getColumnClassName(i);
                final Object value = resultSet.getObject(i);
                addColumn(recordBuilder, name, sqlType, value);
            }
            return recordBuilder.build();
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void addColumn(final Record.Builder builder, final String name, final int sqlType, final Object value) {
        if (value == null) {
            return;
        }
        switch (sqlType) {
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
            builder.withInt(name, (Integer) value);
            break;
        case java.sql.Types.INTEGER:
            if (value instanceof Integer) { // mysql INT can be a java Long
                builder.withInt(name, (Integer) value);
            } else {
                builder.withLong(name, (Long) value);
            }
            break;
        case java.sql.Types.FLOAT:
        case java.sql.Types.REAL:
            builder.withFloat(name, (Float) value);
            break;
        case java.sql.Types.DOUBLE:
            builder.withDouble(name, (Double) value);
            break;
        case java.sql.Types.BOOLEAN:
            builder.withBoolean(name, (Boolean) value);
            break;
        case java.sql.Types.DATE:
            builder.withDateTime(name, new Date(((java.sql.Date) value).getTime()));
            break;
        case java.sql.Types.TIME:
            builder.withTimestamp(name, ((java.sql.Time) value).getTime());
            break;
        case java.sql.Types.TIMESTAMP:
            builder.withTimestamp(name, ((java.sql.Timestamp) value).getTime());
            break;
        case java.sql.Types.BINARY:
        case java.sql.Types.VARBINARY:
        case Types.LONGVARBINARY:
            builder.withBytes(name, (byte[]) value);
            break;
        case java.sql.Types.BIGINT:
        case java.sql.Types.DECIMAL:
        case java.sql.Types.NUMERIC:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CHAR:
        default:
            builder.withString(name, String.valueOf(value));
            break;
        }
    }

    @PreDestroy
    public void release() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.warn(i18n.warnResultSetCantBeClosed(), e);
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.warn(i18n.warnStatementCantBeClosed(), e);
            }
        }
        if (connection != null) {
            try {
                connection.commit();
            } catch (final SQLException e) {
                log.error(i18n.errorSQL(e.getErrorCode(), e.getMessage()), e);
                try {
                    connection.rollback();
                } catch (final SQLException rollbackError) {
                    log.error(i18n.errorSQL(rollbackError.getErrorCode(), rollbackError.getMessage()), rollbackError);
                }
            }
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn(i18n.warnConnectionCantBeClosed(), e);
            }
        }
        if (dataSource != null) {
            dataSource.close();
        }
    }

}
