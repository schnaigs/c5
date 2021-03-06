/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package c5db.client;

import c5db.MiniClusterBase;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * language to help verify data from the database.
 * <p/>
 * Add additional static verbs as necessary!
 */
public class DataHelper {
  private static final byte[] cf = Bytes.toBytes("cf");
  private static final byte[] cq = Bytes.toBytes("cq");

  static byte[][] valuesReadFromDB(C5Table c5Table, byte[][] row) throws IOException {
    List<Get> gets = Arrays.asList(row).stream().map(getRow -> {
      Get get = new Get(getRow);
      get.addColumn(cf, cq);
      return get;
    }).collect(toList());

    Result[] results = c5Table.get(gets);

    List<byte[]> values = Arrays
        .asList(results)
        .stream()
        .map(result -> result.getValue(cf, cq)).collect(toList());
    return values.toArray(new byte[values.size()][]);

  }

  static Boolean[] valuesExistsInDB(C5Table c5Table, byte[][] row) throws IOException {
    List<Get> gets = Arrays.asList(row).stream().map(getRow -> {
      Get get = new Get(getRow);
      get.addColumn(cf, cq);
      return get;
    }).collect(toList());

    return c5Table.exists(gets);
  }

  static byte[] valueReadFromDB(C5Table c5Table, byte[] row) throws IOException {
    Get get = new Get(row);
    get.addColumn(cf, cq);
    Result result = c5Table.get(get);
    return result.getValue(cf, cq);
  }

  static boolean valueExistsInDB(C5Table c5Table, byte[] row) throws IOException {
    Get get = new Get(row);
    get.addColumn(cf, cq);
    return c5Table.exists(get);
  }

  static void putRowInDB(C5Table c5Table, byte[] row) throws IOException {
    Put put = new Put(row);
    put.add(cf, cq, MiniClusterBase.value);
    c5Table.put(put);
  }

  static void putRowAndValueIntoDatabase(C5Table c5Table,
                                         byte[] row,
                                         byte[] valuePutIntoDatabase) throws IOException {
    Put put = new Put(row);
    put.add(cf, cq, valuePutIntoDatabase);
    c5Table.put(put);
  }

  static void putsRowInDB(C5Table c5Table, byte[][] rows, byte[] value) throws IOException {
    Stream<Put> puts = Arrays.asList(rows).stream().map(row -> new Put(row).add(cf, cq, value));
    c5Table.put(puts.collect(toList()));
  }

  static boolean checkAndPutRowAndValueIntoDatabase(C5Table c5Table,
                                                    byte[] row,
                                                    byte[] valueToCheck,
                                                    byte[] valuePutIntoDatabase) throws IOException {
    Put put = new Put(row);
    put.add(cf, cq, valuePutIntoDatabase);
    return c5Table.checkAndPut(row, cf, cq, valueToCheck, put);
  }

  static boolean checkAndDeleteRowAndValueIntoDatabase(C5Table c5Table,
                                                       byte[] row,
                                                       byte[] valueToCheck) throws IOException {
    Delete delete = new Delete(row);
    return c5Table.checkAndDelete(row, cf, cq, valueToCheck, delete);
  }

  static ResultScanner getScanner(C5Table c5Table, byte[] row) throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(cf, cq);
    return c5Table.getScanner(scan);
  }

  static byte[] nextResult(ResultScanner resultScanner) throws IOException {
    return resultScanner.next().getValue(cf, cq);
  }
}
