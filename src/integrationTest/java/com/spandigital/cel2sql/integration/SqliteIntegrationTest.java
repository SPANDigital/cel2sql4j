package com.spandigital.cel2sql.integration;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.sqlite.SqliteDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class SqliteIntegrationTest extends AbstractDialectIntegrationTest {

    private static Connection connection;
    private static final Dialect DIALECT = new SqliteDialect();

    @BeforeAll
    static void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:");
        try (var stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE test_data (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    adult INTEGER NOT NULL,
                    height REAL NOT NULL,
                    active INTEGER NOT NULL,
                    null_var TEXT,
                    string_list TEXT NOT NULL,
                    int_list TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """);
            stmt.execute("""
                INSERT INTO test_data VALUES
                (1, 'Alice',    30, 1, 1.65,         1, 'hello', '["good","bad","ok"]',      '[1,2,3]',   '2024-06-15T10:30:00'),
                (2, 'Bob',      17, 0, 1.80,         0, NULL,    '["good","great"]',          '[4,5]',     '2024-01-01T00:00:00'),
                (3, 'Carol',    25, 1, 1.70,         1, 'world', '["bad"]',                   '[6]',       '2024-03-20T15:45:30'),
                (4, 'Dave',     45, 1, 1.90,         0, NULL,    '["unique","good"]',          '[7,8,9]',   '2023-12-25T08:00:00'),
                (5, 'Eve',      20, 1, 1.6180339887, 1, '',      '["good","bad","unique"]',    '[10]',      '2024-07-04T12:00:00'),
                (6, 'aardvark', 10, 0, 1.40,         0, NULL,    '[]',                         '[]',        '2024-02-29T23:59:59')
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
        return "SQLite";
    }
}
