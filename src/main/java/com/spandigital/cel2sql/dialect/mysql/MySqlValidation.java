package com.spandigital.cel2sql.dialect.mysql;

import com.spandigital.cel2sql.error.ConversionException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * MySQL-specific field name validation and reserved keyword management.
 * Ported from the Go {@code dialect/mysql/validation.go} implementation.
 */
final class MySqlValidation {

    /** Maximum identifier length in MySQL. */
    static final int MAX_IDENTIFIER_LENGTH = 64;

    /** Pattern for valid MySQL identifiers: starts with letter or underscore, then alphanumeric or underscore. */
    private static final Pattern FIELD_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    /**
     * Set of MySQL reserved SQL keywords (lowercased).
     * These cannot be used as unquoted identifiers.
     */
    private static final Set<String> RESERVED_SQL_KEYWORDS;

    static {
        Set<String> keywords = new HashSet<>();
        keywords.add("accessible");
        keywords.add("add");
        keywords.add("all");
        keywords.add("alter");
        keywords.add("analyze");
        keywords.add("and");
        keywords.add("as");
        keywords.add("asc");
        keywords.add("asensitive");
        keywords.add("before");
        keywords.add("between");
        keywords.add("bigint");
        keywords.add("binary");
        keywords.add("blob");
        keywords.add("both");
        keywords.add("by");
        keywords.add("call");
        keywords.add("cascade");
        keywords.add("case");
        keywords.add("change");
        keywords.add("char");
        keywords.add("character");
        keywords.add("check");
        keywords.add("collate");
        keywords.add("column");
        keywords.add("condition");
        keywords.add("constraint");
        keywords.add("continue");
        keywords.add("convert");
        keywords.add("create");
        keywords.add("cross");
        keywords.add("cube");
        keywords.add("cume_dist");
        keywords.add("current_date");
        keywords.add("current_time");
        keywords.add("current_timestamp");
        keywords.add("current_user");
        keywords.add("cursor");
        keywords.add("database");
        keywords.add("databases");
        keywords.add("day_hour");
        keywords.add("day_microsecond");
        keywords.add("day_minute");
        keywords.add("day_second");
        keywords.add("dec");
        keywords.add("decimal");
        keywords.add("declare");
        keywords.add("default");
        keywords.add("delayed");
        keywords.add("delete");
        keywords.add("dense_rank");
        keywords.add("desc");
        keywords.add("describe");
        keywords.add("deterministic");
        keywords.add("distinct");
        keywords.add("distinctrow");
        keywords.add("div");
        keywords.add("double");
        keywords.add("drop");
        keywords.add("dual");
        keywords.add("each");
        keywords.add("else");
        keywords.add("elseif");
        keywords.add("empty");
        keywords.add("enclosed");
        keywords.add("escaped");
        keywords.add("except");
        keywords.add("exists");
        keywords.add("exit");
        keywords.add("explain");
        keywords.add("false");
        keywords.add("fetch");
        keywords.add("first_value");
        keywords.add("float");
        keywords.add("float4");
        keywords.add("float8");
        keywords.add("for");
        keywords.add("force");
        keywords.add("foreign");
        keywords.add("from");
        keywords.add("fulltext");
        keywords.add("function");
        keywords.add("generated");
        keywords.add("get");
        keywords.add("grant");
        keywords.add("group");
        keywords.add("grouping");
        keywords.add("groups");
        keywords.add("having");
        keywords.add("high_priority");
        keywords.add("hour_microsecond");
        keywords.add("hour_minute");
        keywords.add("hour_second");
        keywords.add("if");
        keywords.add("ignore");
        keywords.add("in");
        keywords.add("index");
        keywords.add("infile");
        keywords.add("inner");
        keywords.add("inout");
        keywords.add("insensitive");
        keywords.add("insert");
        keywords.add("int");
        keywords.add("int1");
        keywords.add("int2");
        keywords.add("int3");
        keywords.add("int4");
        keywords.add("int8");
        keywords.add("integer");
        keywords.add("interval");
        keywords.add("into");
        keywords.add("io_after_gtids");
        keywords.add("io_before_gtids");
        keywords.add("is");
        keywords.add("iterate");
        keywords.add("join");
        keywords.add("json_table");
        keywords.add("key");
        keywords.add("keys");
        keywords.add("kill");
        keywords.add("lag");
        keywords.add("last_value");
        keywords.add("lateral");
        keywords.add("lead");
        keywords.add("leading");
        keywords.add("leave");
        keywords.add("left");
        keywords.add("like");
        keywords.add("limit");
        keywords.add("linear");
        keywords.add("lines");
        keywords.add("load");
        keywords.add("localtime");
        keywords.add("localtimestamp");
        keywords.add("lock");
        keywords.add("long");
        keywords.add("longblob");
        keywords.add("longtext");
        keywords.add("loop");
        keywords.add("low_priority");
        keywords.add("master_bind");
        keywords.add("master_ssl_verify_server_cert");
        keywords.add("match");
        keywords.add("maxvalue");
        keywords.add("mediumblob");
        keywords.add("mediumint");
        keywords.add("mediumtext");
        keywords.add("middleint");
        keywords.add("minute_microsecond");
        keywords.add("minute_second");
        keywords.add("mod");
        keywords.add("modifies");
        keywords.add("natural");
        keywords.add("not");
        keywords.add("no_write_to_binlog");
        keywords.add("nth_value");
        keywords.add("ntile");
        keywords.add("null");
        keywords.add("numeric");
        keywords.add("of");
        keywords.add("on");
        keywords.add("optimize");
        keywords.add("optimizer_costs");
        keywords.add("option");
        keywords.add("optionally");
        keywords.add("or");
        keywords.add("order");
        keywords.add("out");
        keywords.add("outer");
        keywords.add("outfile");
        keywords.add("over");
        keywords.add("partition");
        keywords.add("percent_rank");
        keywords.add("precision");
        keywords.add("primary");
        keywords.add("procedure");
        keywords.add("purge");
        keywords.add("range");
        keywords.add("rank");
        keywords.add("read");
        keywords.add("reads");
        keywords.add("read_write");
        keywords.add("real");
        keywords.add("recursive");
        keywords.add("references");
        keywords.add("regexp");
        keywords.add("release");
        keywords.add("rename");
        keywords.add("repeat");
        keywords.add("replace");
        keywords.add("require");
        keywords.add("resignal");
        keywords.add("restrict");
        keywords.add("return");
        keywords.add("revoke");
        keywords.add("right");
        keywords.add("rlike");
        keywords.add("row");
        keywords.add("rows");
        keywords.add("row_number");
        keywords.add("schema");
        keywords.add("schemas");
        keywords.add("second_microsecond");
        keywords.add("select");
        keywords.add("sensitive");
        keywords.add("separator");
        keywords.add("set");
        keywords.add("show");
        keywords.add("signal");
        keywords.add("smallint");
        keywords.add("spatial");
        keywords.add("specific");
        keywords.add("sql");
        keywords.add("sqlexception");
        keywords.add("sqlstate");
        keywords.add("sqlwarning");
        keywords.add("sql_big_result");
        keywords.add("sql_calc_found_rows");
        keywords.add("sql_small_result");
        keywords.add("ssl");
        keywords.add("starting");
        keywords.add("stored");
        keywords.add("straight_join");
        keywords.add("system");
        keywords.add("table");
        keywords.add("terminated");
        keywords.add("then");
        keywords.add("tinyblob");
        keywords.add("tinyint");
        keywords.add("tinytext");
        keywords.add("to");
        keywords.add("trailing");
        keywords.add("trigger");
        keywords.add("true");
        keywords.add("undo");
        keywords.add("union");
        keywords.add("unique");
        keywords.add("unlock");
        keywords.add("unsigned");
        keywords.add("update");
        keywords.add("usage");
        keywords.add("use");
        keywords.add("using");
        keywords.add("utc_date");
        keywords.add("utc_time");
        keywords.add("utc_timestamp");
        keywords.add("values");
        keywords.add("varbinary");
        keywords.add("varchar");
        keywords.add("varcharacter");
        keywords.add("varying");
        keywords.add("virtual");
        keywords.add("when");
        keywords.add("where");
        keywords.add("while");
        keywords.add("window");
        keywords.add("with");
        keywords.add("write");
        keywords.add("xor");
        keywords.add("year_month");
        keywords.add("zerofill");
        RESERVED_SQL_KEYWORDS = Collections.unmodifiableSet(keywords);
    }

    private MySqlValidation() {
        // utility class
    }

    /**
     * Validates a field name for use as a MySQL identifier.
     *
     * @param name the field name to validate
     * @throws ConversionException if the field name is empty, too long, contains invalid characters,
     *                             or is a reserved keyword
     */
    static void validateFieldName(String name) throws ConversionException {
        if (name == null || name.isEmpty()) {
            throw ConversionException.of("field name cannot be empty",
                    "field name cannot be empty");
        }
        if (name.length() > MAX_IDENTIFIER_LENGTH) {
            String detail = String.format(
                    "field name \"%s\" exceeds MySQL maximum identifier length of %d characters",
                    name, MAX_IDENTIFIER_LENGTH);
            throw ConversionException.of("Invalid field name", detail);
        }
        if (!FIELD_NAME_PATTERN.matcher(name).matches()) {
            String detail = String.format(
                    "field name \"%s\" must start with a letter or underscore and contain only alphanumeric characters and underscores",
                    name);
            throw ConversionException.of("Invalid field name", detail);
        }
        if (RESERVED_SQL_KEYWORDS.contains(name.toLowerCase())) {
            String detail = String.format(
                    "field name \"%s\" is a reserved SQL keyword and cannot be used without quoting",
                    name);
            throw ConversionException.of("Invalid field name", detail);
        }
    }

    /**
     * Returns the set of reserved SQL keywords for MySQL.
     *
     * @return an unmodifiable set of lowercased reserved keywords
     */
    static Set<String> getReservedKeywords() {
        return RESERVED_SQL_KEYWORDS;
    }
}
