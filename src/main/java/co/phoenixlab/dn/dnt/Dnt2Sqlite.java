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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.DoubleConsumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class Dnt2Sqlite {

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("SQLite-JDBC driver not found! Make sure sqlite-jdbc is on the classpath.");
        }
    }

    private final DoubleConsumer noOpListener;
    private Connection connection;

    public Dnt2Sqlite(String jdbcUriSuffix) throws SQLException {
        noOpListener = d -> {
        };
        String jdbcUri = "jdbc:sqlite:" + jdbcUriSuffix;
        Properties properties = new Properties();
        properties.put("useUnicode", "true");
        properties.put("characterEncoding", "UTF-8");
        connection = DriverManager.getConnection(jdbcUri, properties);
    }

    public Dnt2Sqlite(Path sqliteFileOut) throws SQLException {
        this(sqliteFileOut.toString().replace('\\', '/'));
    }

    public static void main(String[] args) throws IOException, SQLException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Would you like to collapse common tables (e.g. item)? [Y/(N)]");
        String line = scanner.nextLine();
        boolean collapse = false;
        if (line.equalsIgnoreCase("y")) {
            collapse = true;
        }
        System.out.println("DNT2SQLITE will " + (collapse ? "collapse" : "not collapse") + " common tables.");
        System.out.println("Enter the DNT files to read. Enter \"done\" when finished.");
        List<String> files = new ArrayList<>();
        while (!"done".equals(line = scanner.nextLine())) {
            files.add(line);
        }
        System.out.println("Parsed " + files.size() + " filenames");
        System.out.println("Enter the target SQLite file to dump to");
        String out = scanner.nextLine();
        Path outPath = Paths.get(out);
        Dnt2Sqlite dnt2Sqlite = new Dnt2Sqlite(outPath);
        long startTime = System.currentTimeMillis();
        try {
            Predicate<Path> endsWithDnt = f -> f.getFileName().toString().endsWith(".dnt");
            Predicate<Path> endsWithExt = f -> f.getFileName().toString().endsWith(".ext");
            Predicate<Path> endsWithRightType = endsWithDnt.or(endsWithExt);
            final boolean collapseF = collapse;
            files.stream().
                    map(Paths::get).
                    forEach(p -> {
                        if (Files.isDirectory(p)) {
                            try (Stream<Path> fs = Files.walk(p, 1)) {
                                fs.filter(Files::isRegularFile).
                                        filter(endsWithRightType).
                                        forEach(f -> convert(f, dnt2Sqlite, collapseF));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            convert(p, dnt2Sqlite, collapseF);
                        }
                    });
        } finally {
            System.out.println();
            System.out.printf("Took %,.2f sec\n", (System.currentTimeMillis() - startTime) / 1000D);
            scanner.close();
            dnt2Sqlite.close();
        }
    }

    private static void convert(Path f, Dnt2Sqlite dnt2Sqlite, boolean collapse) {
        try {
            System.out.println();
            long startTime = System.currentTimeMillis();
            dnt2Sqlite.convert(f, d -> System.out.printf("\r[%5.1f%%] Converting %s ",
                    d * 100D, f.getFileName().toString()), collapse);
            System.out.printf("\r[  OK  ] Converting %s (took %,.2f sec)",
                    f.getFileName().toString(), (System.currentTimeMillis() - startTime) / 1000D);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void convert(Path dntFileIn)
            throws SQLException, IOException {
        convert(dntFileIn, noOpListener, false);
    }

    public void convert(Path dntFileIn, DoubleConsumer progressListener, boolean collapse)
            throws SQLException, IOException {
        process(dntFileIn, connection, progressListener, collapse);
    }

    public Connection readDntAsInMemoryDb(Path dntFileIn)
            throws SQLException, IOException {
        return readDntAsInMemoryDb(dntFileIn, noOpListener, false);
    }

    public Connection readDntAsInMemoryDb(Path dntFileIn, DoubleConsumer progressListener, boolean collapse)
            throws SQLException, IOException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
        process(dntFileIn, connection, progressListener, collapse);
        return connection;
    }

    private void process(Path dntFileIn, Connection connection, DoubleConsumer progressListener, boolean collapse)
            throws SQLException, IOException {
        String override = collapse ? getOverride(dntFileIn.getFileName().toString().toLowerCase()) : null;
        Dnt2SqliteReader reader = new Dnt2SqliteReader(dntFileIn, connection, override);
        reader.read(progressListener);
    }

    private void close() throws SQLException {
        connection.close();
    }

    private String getOverride(String filename) {
        if (filename.startsWith("costumemixinfo")) {
            return "costumemixinfotable_virtual";
        }
        if (filename.startsWith("enchanttable")) {
            return "enchanttable_virtual";
        }
        if (filename.startsWith("gachatable")) {
            return "gachatable_virtual";
        }
        if (filename.startsWith("imprintingtable")) {
            return "imprintingtable_virtual";
        }
        if (filename.startsWith("itemtable")) {
            return "itemtable_virtual";
        }
        if (filename.startsWith("itemcompoundtable")) {
            return "itemcompoundtable_virtual";
        }
        if (filename.startsWith("itemdroptable")) {
            return "itemdroptable_virtual";
        }
        if (filename.startsWith("itemdropgrouptable")) {
            return "itemdropgrouptable_virtual";
        }
        if (filename.startsWith("itemgaintable")) {
            return "itemgaintable_virtual";
        }
        if (filename.startsWith("itemperiod")) {
            return "itemperiodtable_virtual";
        }
        if (filename.startsWith("monstertable")) {
            return "monstertable_virtual";
        }
        if (filename.startsWith("monsterweighttable")) {
            return "monsterweighttable_virtual";
        }
        if (filename.startsWith("partstable")) {
            return "partstable_virtual";
        }
        if (filename.startsWith("potentialtable")) {
            return "potentialtable_virtual";
        }
        if (filename.startsWith("potentialjeweltable")) {
            return "potentialjeweltable_virtual";
        }
        if (filename.startsWith("weapontable")) {
            return "weapontable_virtual";
        }
        if (filename.startsWith("wingtable")) {
            return "wingtable_virtual";
        }

        return null;
    }
}
