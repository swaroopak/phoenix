/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.ScanInfoUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.index.IndexMaintainer;

import java.util.List;

public class GlobalIndexScannerHelper {

    public static final String NUM_CONCURRENT_INDEX_VERIFY_THREADS_CONF_KEY = "index.verify.threads.max";
    public static final int DEFAULT_CONCURRENT_INDEX_VERIFY_THREADS = 17;
    public static final String INDEX_VERIFY_ROW_COUNTS_PER_TASK_CONF_KEY = "index.verify.threads.max";
    public static final int DEFAULT_INDEX_VERIFY_ROW_COUNTS_PER_TASK = 2048;

    public static byte[] getIndexRowKey(IndexMaintainer indexMaintainer, final Put dataRow) {
        ValueGetter valueGetter = new IndexRebuildRegionScanner.SimpleValueGetter(dataRow);
        return indexMaintainer.buildRowKey(valueGetter, new ImmutableBytesWritable(dataRow.getRow()),
                null, null, HConstants.LATEST_TIMESTAMP);
    }

    public static long getMaxTimestamp(Mutation m) {
        long ts = 0;
        for (List<Cell> cells : m.getFamilyCellMap().values()) {
            if (cells == null) {
                continue;
            }
            for (Cell cell : cells) {
                if (ts < cell.getTimestamp()) {
                    ts = cell.getTimestamp();
                }
            }
        }
        return ts;
    }

    public static long getTimestamp(Mutation m) {
        for (List<Cell> cells : m.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                return cell.getTimestamp();
            }
        }
        throw new IllegalStateException("No cell found");
    }

    protected static boolean isTimestampBeforeTTL(long tableTTL, long currentTime, long tsToCheck) {
        if (tableTTL == HConstants.FOREVER) {
            return false;
        }
        return tsToCheck < (currentTime - tableTTL * 1000);
    }

    protected static boolean isTimestampBeyondMaxLookBack(long maxLookBackInMills,
            long currentTime, long tsToCheck) {
        if (!ScanInfoUtil.isMaxLookbackTimeEnabled(maxLookBackInMills)) {
            return false;
        }
        return tsToCheck < (currentTime - maxLookBackInMills);
    }

    protected static long getMaxTimestamp(Pair<Put, Delete> pair) {
        Put put = pair.getFirst();
        long ts1 = 0;
        if (put != null) {
            ts1 = getMaxTimestamp(put);
        }
        Delete del = pair.getSecond();
        long ts2 = 0;
        if (del != null) {
            ts1 = getMaxTimestamp(del);
        }
        return (ts1 > ts2) ? ts1 : ts2;
    }

}
