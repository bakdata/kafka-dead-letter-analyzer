/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StackTraceClassifierTest {

    static final String STACK_TRACE = "org.jdbi.v3.core.ConnectionException: java.sql"
            + ".SQLTransientConnectionException: HikariPool-1 - Connection is not available, "
            + "request timed out after 30000ms.\n\tat org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)"
            + "\n\tat org.jdbi.v3.core.LazyHandleSupplier.initHandle(LazyHandleSupplier"
            + ".java:65)\n\tat org.jdbi.v3.core.LazyHandleSupplier.getHandle(LazyHandleSupplier"
            + ".java:53)\n\tat org.jdbi.v3.sqlobject.statement.internal"
            + ".CustomizingStatementHandler.invoke(CustomizingStatementHandler.java:171)\n\tat "
            + "org.jdbi.v3.sqlobject.statement.internal.SqlQueryHandler.invoke(SqlQueryHandler"
            + ".java:27)\n\tat org.jdbi.v3.sqlobject.internal.SqlObjectInitData$1"
            + ".lambda$invoke$0(SqlObjectInitData.java:132)\n\tat org.jdbi.v3.core.internal"
            + ".Invocations.invokeWith(Invocations.java:44)\n\tat org.jdbi.v3.core.internal"
            + ".Invocations.invokeWith(Invocations.java:26)\n\tat org.jdbi.v3.core"
            + ".LazyHandleSupplier.lambda$invokeInContext$1(LazyHandleSupplier.java:77)\n\tat "
            + "org.jdbi.v3.core.internal.Invocations.invokeWith(Invocations.java:44)\n\tat org"
            + ".jdbi.v3.core.internal.Invocations.invokeWith(Invocations.java:26)\n\tat org"
            + ".jdbi.v3.core.LazyHandleSupplier.invokeInContext(LazyHandleSupplier.java:76)"
            + "\n\tat org.jdbi.v3.sqlobject.internal.SqlObjectInitData$1.call(SqlObjectInitData"
            + ".java:138)\n\tat org.jdbi.v3.sqlobject.internal.SqlObjectInitData$1.invoke"
            + "(SqlObjectInitData.java:132)\n\tat org.jdbi.v3.sqlobject.SqlObjectFactory"
            + ".lambda$attach$2(SqlObjectFactory.java:110)\n\tat com.sun.proxy.$Proxy41"
            + ".findById(Unknown Source)\n\t... 40 more\n";

    static Stream<Arguments> generateStackTraces() {
        return Stream.of(
                Arguments.of(
                        "io.confluent.connect.elasticsearch.ElasticsearchClient$ReportingException: Indexing failed: "
                                + "ElasticsearchException[Elasticsearch exception [type=mapper_parsing_exception, "
                                + "reason=failed to parse field [timestamp] of type [date] in document with id "
                                + "'brs10610'. Preview of field's value: '20120515']]; nested: "
                                + "ElasticsearchException[Elasticsearch exception [type=illegal_argument_exception, "
                                + "reason=failed to parse date field [20120515] with format [yyyy]]]; nested: "
                                + "ElasticsearchException[Elasticsearch exception [type=date_time_parse_exception, "
                                + "reason=date_time_parse_exception: Text '20120515' could not be parsed at index "
                                + "0]];\n",
                        "io.confluent.connect.elasticsearch.ElasticsearchClient$ReportingException"),
                Arguments.of("java.lang.NullPointerException\n", "java.lang.NullPointerException"),
                Arguments.of(STACK_TRACE, "org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)")
        );
    }

    @ParameterizedTest
    @MethodSource("generateStackTraces")
    void shouldParseLine(final String stackTrace, final String expectedLine) {
        assertThat(StackTraceClassifier.classify(stackTrace)).isEqualTo(expectedLine);
    }

}
