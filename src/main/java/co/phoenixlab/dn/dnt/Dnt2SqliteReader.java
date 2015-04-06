/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Vincent Zhang/PhoenixLAB
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package co.phoenixlab.dn.dnt;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;
import java.util.StringJoiner;
import java.util.function.DoubleConsumer;

class Dnt2SqliteReader {

    enum DataType {
        UINT32("UNSIGNED INT"),
        INT32("INT"),
        BOOL("BIT"),
        FLOAT,
        DOUBLE,
        STRING("TEXT");

        private DataType() {
            this.sqlName = this.name();
        }

        private DataType(String sqlName) {
            this.sqlName = sqlName;
        }

        public final String sqlName;

        @Override
        public String toString() {
            return sqlName;
        }

        public static DataType fromId(int id) {
            switch (id) {
                case 1:
                    return STRING;
                case 2:
                    return BOOL;
                case 3:
                    return INT32;
                case 4:
                    return FLOAT;
                case 5:
                    return DOUBLE;
                default:
                    throw new NoSuchElementException("No DataType with ID " + id);
            }
        }
    }

    private static final int MAGIC_NUMBER = 0x00000000;

    private final Path dntFile;
    private final Connection dbConnection;
    private byte[] stringBytes;

    public Dnt2SqliteReader(Path dntFile, Connection dbConnection) {
        this.dntFile = dntFile;
        this.dbConnection = dbConnection;
        this.stringBytes = new byte[1024];
    }

    public void read(DoubleConsumer progressListener)
            throws SQLException, IOException {
        progressListener.accept(0D);
        try (LittleEndianDataInputStream inputStream = new LittleEndianDataInputStream(
                new DataInputStream(
                        Files.newInputStream(dntFile, StandardOpenOption.READ)))) {
            validateMagicNumber(inputStream.readInt());
            Column[] columns = new Column[inputStream.readUnsignedShort() + 1];
            long rowCount = inputStream.readUnsignedInt();
            readColumnHeaders(inputStream, columns);
            dropTableIfExistsAndCreate(columns);
            readRows(progressListener, inputStream, columns, rowCount);
            progressListener.accept(1D);
        }
    }

    private void readRows(DoubleConsumer progressListener, LittleEndianDataInputStream inputStream, Column[] columns, long rowCount) throws SQLException, IOException {
        dbConnection.setAutoCommit(false);
        StringJoiner joiner = new StringJoiner(",", "INSERT INTO Data VALUES(", ");");
        for (int i = 0; i < columns.length; i++) {
            joiner.add("?");
        }
        long interval = Math.max(1, rowCount / 20);
        try (PreparedStatement statement = dbConnection.prepareStatement(joiner.toString())) {
            for (long row = 0; row < rowCount; row++) {
                readRowData(inputStream, columns, statement);
                statement.executeUpdate();
                if (row % interval == 0) {
                    dbConnection.commit();
                }
                progressListener.accept((double) row / (double) rowCount);
            }
        }
        dbConnection.commit();
        dbConnection.setAutoCommit(true);
    }

    private void readRowData(LittleEndianDataInputStream inputStream, Column[] columns, PreparedStatement statement) throws SQLException, IOException {
        for (int i = 1; i <= columns.length; i++) {
            Column column = columns[i - 1];
            switch (column.dataType) {
                case DOUBLE:
                    statement.setDouble(i, inputStream.readFloat());
                    break;
                case FLOAT:
                    statement.setFloat(i, inputStream.readFloat());
                    break;
                case INT32:
                    statement.setInt(i, inputStream.readInt());
                    break;
                case BOOL:
                    statement.setInt(i, inputStream.readInt());
                    break;
                case STRING:
                    int len = inputStream.readUnsignedShort();
                    if (len > stringBytes.length) {
                        stringBytes = new byte[len];
                    }
                    int read;
                    int last = 0;
                    while ((read = inputStream.read(stringBytes, last, len - last)) > 0) {
                        last += read;
                    }
                    statement.setString(i, new String(stringBytes, 0, len, StandardCharsets.UTF_8));
                    break;
                case UINT32:
                    statement.setInt(i, inputStream.readInt());
                    break;
                default:
                    throw new IllegalStateException("This shouldn't happen, this is for happy compiler");
            }
        }
    }

    private void validateMagicNumber(int magic) throws IOException {
        if (magic != MAGIC_NUMBER) {
            throw new InvalidDntException(dntFile,
                    String.format("Magic number mismatch, expected 0x%08X, got 0x%08X",
                            MAGIC_NUMBER,
                            magic));
        }
    }

    private void readColumnHeaders(LittleEndianDataInputStream inputStream, Column[] columns) throws IOException {
        columns[0] = new Column("RowId", DataType.INT32);
        for (int i = 1; i < columns.length; i++) {
            int nameLen = inputStream.readUnsignedShort();
            byte[] stringBytes = new byte[nameLen];
            inputStream.readFully(stringBytes);
            String name = new String(stringBytes, StandardCharsets.UTF_8);
            DataType dataType = DataType.fromId(inputStream.readUnsignedByte());
            columns[i] = new Column(name, dataType);
        }
    }

    private void dropTableIfExistsAndCreate(Column[] columns) throws SQLException {
        StringJoiner createTableJoiner = new StringJoiner(", ", "CREATE TABLE Data (", ");");
        for (Column column : columns) {
            createTableJoiner.add(column.name + " " + column.dataType);
        }
        //  Drop existing data table and create new data table
        try (Statement statement = dbConnection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS Data;");
            statement.executeUpdate(createTableJoiner.toString());
        }
    }

    class Column {
        final String name;
        final DataType dataType;

        public Column(String name, DataType dataType) {
            this.name = name;
            this.dataType = dataType;
        }

        @Override
        public String toString() {
            return String.format("name=%s type=%s", name, dataType.name());
        }
    }
}
