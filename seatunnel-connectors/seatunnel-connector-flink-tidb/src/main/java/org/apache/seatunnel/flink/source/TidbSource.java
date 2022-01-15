/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.flink.source;

import static io.tidb.bigdata.tidb.ClientConfig.DATABASE_URL;
import static io.tidb.bigdata.tidb.ClientConfig.USERNAME;
import static java.lang.String.format;

import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.ColumnHandleInternal;
import io.tidb.bigdata.tidb.TableHandleInternal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

public class TidbSource implements FlinkBatchSource<Row> {

    public static final String DATABASE_NAME = "tidb.database.name";
    public static final String TABLE_NAME = "tidb.table.name";
    public static final String FIELD_NAME = "field_name";
    public static final String LIMIT = "limit";

    private Config config;
    private String databaseName;
    private String tableName;
    private String[] fieldNames;
    private DataType[] fieldTypes;
    private Map<String, String> properties;
    private TidbInputFormat tidbInputFormat;
    private int[] projectionFields;
    private Long limit = Long.MAX_VALUE;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.check(config, DATABASE_URL, USERNAME, DATABASE_NAME, TABLE_NAME);
    }

    private void querySchema() {
        try (ClientSession session = ClientSession.createWithSingleConnection(new ClientConfig(properties))) {
            // check exist
            session.getTableMust(databaseName, tableName);
            TableHandleInternal tableHandleInternal = new TableHandleInternal(
                    UUID.randomUUID().toString(), this.databaseName, this.tableName);
            List<ColumnHandleInternal> columns = session.getTableColumns(tableHandleInternal)
                    .orElseThrow(() -> new NullPointerException("columnHandleInternals is null"));
            this.fieldNames = columns.stream().map(ColumnHandleInternal::getName).toArray(String[]::new);
            this.fieldTypes = columns.stream().map(column -> TypeUtils.getFlinkType(column.getType())).toArray(DataType[]::new);
            Optional<String> projection = Optional.ofNullable(properties.get(FIELD_NAME));
            if (projection.isPresent()) {
                Map<String, Integer> nameAndIndex = new HashMap<>(fieldNames.length);
                IntStream.range(0, columns.size())
                        .forEach(i -> nameAndIndex.put(columns.get(i).getName(), i));
                this.projectionFields = Arrays.stream(projection.get().split(","))
                        .map(name -> Objects.requireNonNull(nameAndIndex.get(name),
                                format("can not find column: %s in table `%s`.`%s`", name, databaseName, tableName)))
                        .mapToInt(i -> i)
                        .toArray();
            }
            Optional.ofNullable(properties.get(LIMIT)).ifPresent(s -> limit = Long.parseLong(s));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        this.databaseName = config.getString(DATABASE_NAME);
        this.tableName = config.getString(TABLE_NAME);
        this.properties = config.entrySet().stream()
                .collect(Collectors.toMap(e -> StringUtils.strip(e.getKey(), "\""), e -> config.getString(e.getKey())));
        querySchema();
        this.tidbInputFormat = new TidbInputFormat(properties, fieldNames, fieldTypes);
        Optional.ofNullable(projectionFields).ifPresent(tidbInputFormat::setProjectedFields);
        tidbInputFormat.setLimit(limit);
    }

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        return env.getBatchEnvironment().createInput(tidbInputFormat, tidbInputFormat.getProducedType());
    }
}
