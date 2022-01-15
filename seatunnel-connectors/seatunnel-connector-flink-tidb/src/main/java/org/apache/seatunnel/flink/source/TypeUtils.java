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

import static java.lang.String.format;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.tikv.common.types.StringType;

public class TypeUtils {

    public static DataType getFlinkType(org.tikv.common.types.DataType dataType) {
        boolean unsigned = dataType.isUnsigned();
        int length = (int) dataType.getLength();
        switch (dataType.getType()) {
            case TypeBit:
                return DataTypes.BOOLEAN();
            case TypeTiny:
                return unsigned ? DataTypes.SMALLINT() : DataTypes.TINYINT();
            case TypeYear:
            case TypeShort:
                return unsigned ? DataTypes.INT() : DataTypes.SMALLINT();
            case TypeInt24:
            case TypeLong:
                return unsigned ? DataTypes.BIGINT() : DataTypes.INT();
            case TypeLonglong:
                return unsigned ? DataTypes.DECIMAL(length, 0) : DataTypes.BIGINT();
            case TypeFloat:
                return DataTypes.FLOAT();
            case TypeDouble:
                return DataTypes.DOUBLE();
            case TypeNull:
                return DataTypes.NULL();
            case TypeDatetime:
            case TypeTimestamp:
                return DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class);
            case TypeDate:
            case TypeNewDate:
                return DataTypes.DATE().bridgedTo(Date.class);
            case TypeDuration:
                return DataTypes.TIME().bridgedTo(Time.class);
            case TypeTinyBlob:
            case TypeMediumBlob:
            case TypeLongBlob:
            case TypeBlob:
            case TypeVarString:
            case TypeString:
            case TypeVarchar:
                if (dataType instanceof StringType) {
                    return DataTypes.STRING();
                }
                return DataTypes.BYTES();
            case TypeJSON:
            case TypeEnum:
            case TypeSet:
                return DataTypes.STRING();
            case TypeDecimal:
            case TypeNewDecimal:
                return DataTypes.DECIMAL(length, dataType.getDecimal());
            case TypeGeometry:
            default:
                throw new IllegalArgumentException(
                        format("can not get flink datatype by tikv type: %s", dataType));
        }
    }

    public static Object covertObject(Object object, org.tikv.common.types.DataType dataType) {
        if (object == null) {
            return null;
        }
        boolean unsigned = dataType.isUnsigned();
        switch (dataType.getType()) {
            case TypeBit:
                return Integer.parseInt(object.toString()) == 1;
            case TypeTiny:
            case TypeYear:
            case TypeShort:
                return Integer.parseInt(object.toString());
            case TypeInt24:
            case TypeLong:
                return unsigned ? (long) object : Integer.parseInt(object.toString());
            case TypeLonglong:
                return unsigned ? (BigDecimal) object : (long) object;
            case TypeFloat:
                return (float) (double) object;
            case TypeDouble:
                return (double) object;
            case TypeNull:
                return null;
            case TypeDatetime:
            case TypeTimestamp:
                return new Timestamp(((long) object) / 1000);
            case TypeDate:
            case TypeNewDate:
                return Date.valueOf(LocalDate.ofEpochDay(Long.parseLong(object.toString())));
            case TypeDuration:
                return Time.valueOf(LocalTime.ofNanoOfDay(Long.parseLong(object.toString())));
            case TypeTinyBlob:
            case TypeMediumBlob:
            case TypeLongBlob:
            case TypeBlob:
            case TypeVarString:
            case TypeString:
            case TypeVarchar:
                if (dataType instanceof StringType) {
                    return object.toString();
                }
                return (byte[]) object;
            case TypeJSON:
            case TypeEnum:
            case TypeSet:
                return object.toString();
            case TypeDecimal:
            case TypeNewDecimal:
                return (BigDecimal) object;
            case TypeGeometry:
            default:
                throw new IllegalArgumentException(
                        format("Can not covert tikv type to flink type, object = %s, type = %s", object,
                                dataType));
        }
    }
}
