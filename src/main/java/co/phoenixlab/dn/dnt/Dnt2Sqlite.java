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

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.DoubleConsumer;

public class Dnt2Sqlite {

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("SQLite-JDBC driver not found! Make sure sqlite-jdbc is on the classpath.");
        }
    }

    public static void main(String[] args) {

    }

    private final DoubleConsumer noOpListener;

    public Dnt2Sqlite() {
        noOpListener = d -> {
        };
    }

    public void convert(Path dntFileIn, Path sqliteFileOut)
            throws SQLException, IOException {
        convert(dntFileIn, sqliteFileOut, noOpListener);
    }

    public void convert(Path dntFileIn, Path sqliteFileOut, DoubleConsumer progressListener)
            throws SQLException, IOException {
        String jdbcUri = "jdbc:sqlite:" + sqliteFileOut.toString().replace('\\', '/');
        try (Connection connection = DriverManager.getConnection(jdbcUri)) {
            process(dntFileIn, connection, progressListener);
        }
    }

    public Connection readDntAsInMemoryDb(Path dntFileIn)
            throws SQLException, IOException {
        return readDntAsInMemoryDb(dntFileIn, noOpListener);
    }

    public Connection readDntAsInMemoryDb(Path dntFileIn, DoubleConsumer progressListener)
            throws SQLException, IOException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
        process(dntFileIn, connection, progressListener);
        return connection;
    }

    private void process(Path dntFileIn, Connection connection, DoubleConsumer progressListener)
            throws SQLException, IOException {
        Dnt2SqliteReader reader = new Dnt2SqliteReader(dntFileIn, connection);
        reader.read(progressListener);
    }
}
