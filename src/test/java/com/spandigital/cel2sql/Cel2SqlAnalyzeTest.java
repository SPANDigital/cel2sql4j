package com.spandigital.cel2sql;

import com.spandigital.cel2sql.dialect.IndexRecommendation;
import com.spandigital.cel2sql.dialect.bigquery.BigQueryDialect;
import com.spandigital.cel2sql.dialect.duckdb.DuckDbDialect;
import com.spandigital.cel2sql.dialect.mysql.MySqlDialect;
import com.spandigital.cel2sql.dialect.postgres.PostgresDialect;
import com.spandigital.cel2sql.dialect.sqlite.SqliteDialect;
import com.spandigital.cel2sql.testutil.CelHelper;
import dev.cel.common.CelAbstractSyntaxTree;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the query analysis and index recommendation feature.
 */
class Cel2SqlAnalyzeTest {

    @Test
    @DisplayName("PostgreSQL: comparison yields BTREE recommendation")
    void postgresComparisonIndex() throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile("age > 20");
        AnalyzeResult result = Cel2Sql.analyzeQuery(ast, opts -> opts.withDialect(new PostgresDialect()));

        assertThat(result.sql()).isEqualTo("age > 20");
        assertThat(result.recommendations()).hasSize(1);

        IndexRecommendation rec = result.recommendations().get(0);
        assertThat(rec.column()).isEqualTo("age");
        assertThat(rec.indexType()).isEqualTo("BTREE");
        assertThat(rec.expression()).contains("CREATE INDEX");
    }

    @Test
    @DisplayName("PostgreSQL: regex yields GIN recommendation")
    void postgresRegexIndex() throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile("name.matches(\"a+\")");
        AnalyzeResult result = Cel2Sql.analyzeQuery(ast, opts -> opts.withDialect(new PostgresDialect()));

        assertThat(result.recommendations()).hasSize(1);
        IndexRecommendation rec = result.recommendations().get(0);
        assertThat(rec.column()).isEqualTo("name");
        assertThat(rec.indexType()).isEqualTo("GIN");
        assertThat(rec.expression()).contains("gin_trgm_ops");
    }

    @Test
    @DisplayName("MySQL: comparison yields BTREE recommendation")
    void mysqlComparisonIndex() throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile("age == 25");
        AnalyzeResult result = Cel2Sql.analyzeQuery(ast, opts -> opts.withDialect(new MySqlDialect()));

        assertThat(result.recommendations()).hasSize(1);
        IndexRecommendation rec = result.recommendations().get(0);
        assertThat(rec.indexType()).isEqualTo("BTREE");
    }

    @Test
    @DisplayName("DuckDB: comparison yields ART recommendation")
    void duckdbComparisonIndex() throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile("age < 30");
        AnalyzeResult result = Cel2Sql.analyzeQuery(ast, opts -> opts.withDialect(new DuckDbDialect()));

        assertThat(result.recommendations()).hasSize(1);
        IndexRecommendation rec = result.recommendations().get(0);
        assertThat(rec.indexType()).isEqualTo("ART");
    }

    @Test
    @DisplayName("BigQuery: comparison yields CLUSTERING recommendation")
    void bigqueryComparisonIndex() throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile("age >= 18");
        AnalyzeResult result = Cel2Sql.analyzeQuery(ast, opts -> opts.withDialect(new BigQueryDialect()));

        assertThat(result.recommendations()).hasSize(1);
        IndexRecommendation rec = result.recommendations().get(0);
        assertThat(rec.indexType()).isEqualTo("CLUSTERING");
    }

    @Test
    @DisplayName("SQLite: comparison yields BTREE recommendation")
    void sqliteComparisonIndex() throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile("name == \"test\"");
        AnalyzeResult result = Cel2Sql.analyzeQuery(ast, opts -> opts.withDialect(new SqliteDialect()));

        assertThat(result.recommendations()).hasSize(1);
        IndexRecommendation rec = result.recommendations().get(0);
        assertThat(rec.indexType()).isEqualTo("BTREE");
    }

    @Test
    @DisplayName("Multiple columns yield multiple recommendations")
    void multipleColumns() throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile("name == \"a\" && age > 20");
        AnalyzeResult result = Cel2Sql.analyzeQuery(ast, opts -> opts.withDialect(new PostgresDialect()));

        assertThat(result.sql()).isEqualTo("name = 'a' AND age > 20");
        assertThat(result.recommendations()).hasSize(2);
    }

    @Test
    @DisplayName("No index recommendations for simple literals")
    void noRecommendationsForLiterals() throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile("true");
        AnalyzeResult result = Cel2Sql.analyzeQuery(ast, opts -> opts.withDialect(new PostgresDialect()));

        assertThat(result.sql()).isEqualTo("TRUE");
        assertThat(result.recommendations()).isEmpty();
    }

    @Test
    @DisplayName("Comprehension yields array comprehension recommendation")
    void comprehensionIndex() throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile("string_list.all(x, x != \"bad\")");
        AnalyzeResult result = Cel2Sql.analyzeQuery(ast, opts -> opts.withDialect(new PostgresDialect()));

        assertThat(result.recommendations()).anySatisfy(rec -> {
            assertThat(rec.column()).isEqualTo("string_list");
            assertThat(rec.indexType()).isEqualTo("GIN");
        });
    }
}
