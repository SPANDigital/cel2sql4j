package com.spandigital.cel2sql.integration;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.postgres.PostgresDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Testcontainers
class PostgresIntegrationTest extends AbstractDialectIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("cel2sql_test")
        .withUsername("test")
        .withPassword("test");

    private static Connection connection;
    private static final Dialect DIALECT = new PostgresDialect();

    @BeforeAll
    static void setUp() throws SQLException {
        connection = DriverManager.getConnection(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        try (var stmt = connection.createStatement()) {
            stmt.execute("SET TIMEZONE TO 'UTC'");
            stmt.execute("""
                CREATE TABLE test_data (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    age BIGINT NOT NULL,
                    adult BOOLEAN NOT NULL,
                    height DOUBLE PRECISION NOT NULL,
                    active BOOLEAN NOT NULL,
                    null_var TEXT,
                    string_list TEXT[] NOT NULL,
                    int_list INTEGER[] NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL
                )
                """);
            stmt.execute("""
                INSERT INTO test_data VALUES
                (1, 'Alice',    30, TRUE,  1.65,         TRUE,  'hello', ARRAY['good','bad','ok'],      ARRAY[1,2,3],   '2024-06-15 10:30:00+00'),
                (2, 'Bob',      17, FALSE, 1.80,         FALSE, NULL,    ARRAY['good','great'],          ARRAY[4,5],     '2024-01-01 00:00:00+00'),
                (3, 'Carol',    25, TRUE,  1.70,         TRUE,  'world', ARRAY['bad'],                   ARRAY[6],       '2024-03-20 15:45:30+00'),
                (4, 'Dave',     45, TRUE,  1.90,         FALSE, NULL,    ARRAY['unique','good'],         ARRAY[7,8,9],   '2023-12-25 08:00:00+00'),
                (5, 'Eve',      20, TRUE,  1.6180339887, TRUE,  '',      ARRAY['good','bad','unique'],   ARRAY[10],      '2024-07-04 12:00:00+00'),
                (6, 'aardvark', 10, FALSE, 1.40,         FALSE, NULL,    ARRAY[]::TEXT[],                ARRAY[]::INT[], '2024-02-29 23:59:59+00')
                """);
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    @Override
    protected Connection getConnection() {
        return connection;
    }

    @Override
    protected Dialect getDialect() {
        return DIALECT;
    }

    @Override
    protected String getDialectName() {
        return "PostgreSQL";
    }
}
