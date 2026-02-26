package com.spandigital.cel2sql.integration;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.duckdb.DuckDbDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class DuckDbIntegrationTest extends AbstractDialectIntegrationTest {

    private static Connection connection;
    private static final Dialect DIALECT = new DuckDbDialect();

    @BeforeAll
    static void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        try (var stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE test_data (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    age BIGINT NOT NULL,
                    adult BOOLEAN NOT NULL,
                    height DOUBLE NOT NULL,
                    active BOOLEAN NOT NULL,
                    null_var VARCHAR,
                    string_list VARCHAR[] NOT NULL,
                    int_list INTEGER[] NOT NULL,
                    created_at TIMESTAMP NOT NULL
                )
                """);
            stmt.execute("INSERT INTO test_data VALUES (1, 'Alice',    30, TRUE,  1.65,         TRUE,  'hello', ['good','bad','ok'],      [1,2,3],   TIMESTAMP '2024-06-15 10:30:00')");
            stmt.execute("INSERT INTO test_data VALUES (2, 'Bob',      17, FALSE, 1.80,         FALSE, NULL,    ['good','great'],          [4,5],     TIMESTAMP '2024-01-01 00:00:00')");
            stmt.execute("INSERT INTO test_data VALUES (3, 'Carol',    25, TRUE,  1.70,         TRUE,  'world', ['bad'],                   [6],       TIMESTAMP '2024-03-20 15:45:30')");
            stmt.execute("INSERT INTO test_data VALUES (4, 'Dave',     45, TRUE,  1.90,         FALSE, NULL,    ['unique','good'],         [7,8,9],   TIMESTAMP '2023-12-25 08:00:00')");
            stmt.execute("INSERT INTO test_data VALUES (5, 'Eve',      20, TRUE,  1.6180339887, TRUE,  '',      ['good','bad','unique'],   [10],      TIMESTAMP '2024-07-04 12:00:00')");
            stmt.execute("INSERT INTO test_data VALUES (6, 'aardvark', 10, FALSE, 1.40,         FALSE, NULL,    [],                        [],        TIMESTAMP '2024-02-29 23:59:59')");
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
        return "DuckDB";
    }
}
