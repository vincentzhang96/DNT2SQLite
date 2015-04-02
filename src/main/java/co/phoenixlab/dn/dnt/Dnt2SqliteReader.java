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

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.NoSuchElementException;
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

    private static final int HEADER_SIZE = 10;
    private static final int MAGIC_NUMBER = 0x00000000;

    private final Path dntFile;
    private final Connection dbConnection;

    public Dnt2SqliteReader(Path dntFile, Connection dbConnection) {
        this.dntFile = dntFile;
        this.dbConnection = dbConnection;
    }

    public void read(DoubleConsumer progressListener)
            throws SQLException, IOException {
        progressListener.accept(0D);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(dntFile.toFile(), "r")) {
            FileChannel fileChannel = randomAccessFile.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(0xFFFF).order(ByteOrder.LITTLE_ENDIAN);
            ByteBuffer slicedBuffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
            slicedBuffer.limit(HEADER_SIZE);
            fillBuffer(fileChannel, slicedBuffer);
            int magic = slicedBuffer.getInt();
            if (magic != MAGIC_NUMBER) {
                throw new InvalidDntException(dntFile,
                        String.format("Magic number mismatch, expected 0x%08X, got 0x%08X",
                                MAGIC_NUMBER,
                                magic));
            }
            int numColumns = Short.toUnsignedInt(slicedBuffer.getShort());
            long rowCount = Integer.toUnsignedLong(slicedBuffer.getInt());
            Column[] columns = new Column[numColumns];
            for (int i = 0; i < numColumns; i++) {
                int nameLen = randomAccessFile.readUnsignedShort();
                nameLen = (nameLen & 0xFF) << 8 | ((nameLen >> 8) & 0xFF);   //  Endian swap
                slicedBuffer.rewind();
                slicedBuffer.limit(nameLen + 1);
                fillBuffer(fileChannel, slicedBuffer);
                byte[] stringBytes = new byte[nameLen];
                slicedBuffer.get(stringBytes);
                String name = new String(stringBytes, StandardCharsets.UTF_8);
                DataType dataType = DataType.fromId(slicedBuffer.get());
                columns[i] = new Column(name, dataType);
            }
        } catch (EOFException eof) {
            throw new InvalidDntException(dntFile, "Unexpected EOF");
        }
    }

    private int fillBuffer(SeekableByteChannel byteChannel, ByteBuffer buffer) throws IOException {
        int bytesRead = 0;
        int read;
        while ((read = byteChannel.read(buffer)) > 0) {
            bytesRead += read;
        }
        buffer.rewind();
        if (read == -1) {
            throw new EOFException();
        }
        return bytesRead;
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
