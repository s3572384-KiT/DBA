import model.Count;
import model.DateTime;
import model.Sensor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * important - code reference:
 * https://www.tutorialspoint.com/java/java_documentation.htm
 *
 * @author Kit T
 * @version 1.0
 * @since 12-April-2021
 */
public class Derby {
    private String[] months;
    private String[] days;
    private List<Count> countList;
    private List<DateTime> dateTimeList;
    private Set<Integer> dateIdSet;
    private List<Sensor> sensorList;
    private Set<Integer> sensorIdSet;
    private String dateTable;
    private String sensorTable;
    private String countTable;
    private String[] tables;
    private String[][] sqlList;

    /**
     * convert the month or day into respective number, "January" -> 1, "Wednesday" -> 3
     *
     * @param key String description for month or day, e.g. "January", "Wednesday"
     * @param map String array of months or days
     * @return the respective number for the month or day
     */
    private static int getInt(String key, String[] map) {
        int len = map.length;
        for (int i = 0; i < len; i++) {
            if (map[i].equalsIgnoreCase(key)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * convert String to number
     *
     * @param str to be converted String
     * @return the number after conversion
     */
    private static int toInt(String str) {
        return Integer.parseInt(str);
    }

    /**
     * Drop table if exists
     * important code reference:
     * http://somesimplethings.blogspot.com/2010/03/derby-create-table-if-not-exists.html
     *
     * @param table table name
     * @param conn  connection to the Database
     * @param state statement
     * @throws SQLException exception when invalid sql
     */
    private static void dropTable(String table, Connection conn, Statement state) throws SQLException {
        DatabaseMetaData databaseMetadata = conn.getMetaData();
        ResultSet resultSet;
        resultSet = databaseMetadata.getTables(null, null, table, null);
        if (resultSet.next()) {
            String sql = String.format("drop table %s", table);
            state.execute(sql);
            System.out.println("Dropped table: " + table);
        }
    }

    /**
     * Derby Database loading program driver function
     *
     * @param args command line arguments - an array of String arguments
     */
    public static void main(String[] args) {
        final String path = "../count.csv";
        File file = new File(path);
        if (!file.exists()) {
            System.err.println("CSV file does not exist ...");
            return;
        }
        Derby derby = new Derby();
        derby.init();
        derby.loadDate(path);
    }

    /**
     * Load data from csv file into memory, structure the data into 3 entities:
     * 1. count entity
     * 2. date time entity
     * 3. sensor entity
     *
     * @param path the directory path leads to the source file
     */
    private void loadDate(String path) {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            final String comma = ",";
            boolean isHeader = true;
            String record;
            String[] data;

            // record attributes
            int countId, dateId, year, month, date, day, time, sensorId, hourlyCounts;
            String dateDesc, yearStr, monthStr, dateStr, dayStr, timeStr, sensorName;

            // attributes index
            int countIdIdx = 0, dateTimeIdx = 1, yearIdx = 2, monthIdx = 3, dateIdx = 4;
            int dayIdx = 5, timeIdx = 6, sensorIdIdx = 7, sensorNameIdx = 8, hourlyCountsIdx = 9;

            while ((record = br.readLine()) != null) {
                // skip the 1st line which is header
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                data = record.split(comma);

                // extract the each attribute from the record
                countId = toInt(data[countIdIdx].trim());
                dateDesc = data[dateTimeIdx].trim();
                yearStr = data[yearIdx].trim();
                monthStr = data[monthIdx].trim();
                dateStr = data[dateIdx].trim();
                dayStr = data[dayIdx].trim();
                timeStr = data[timeIdx].trim();
                sensorId = toInt(data[sensorIdIdx].trim());
                sensorName = data[sensorNameIdx].trim();
                hourlyCounts = toInt(data[hourlyCountsIdx].trim());

                year = toInt(yearStr);
                date = toInt(dateStr);
                time = toInt(timeStr);
                month = getInt(monthStr, months);
                day = getInt(dayStr, days);

                // use year + month + date + time for datetime id
                // e.g. year - 2009, month - 10, date - 6, time - 9
                // datetime id will be 2009100609
                dateId = toInt(yearStr + (month < 10 ? "0" + month : month)
                        + (date < 10 ? "0" + date : date) + (time < 10 ? "0" + time : time));

                addToDateList(dateId, dateDesc, year, month, date, day, time);
                addToSensorList(sensorId, sensorName);
                addToCountList(countId, hourlyCounts, dateId, sensorId);
            }

            // sort all the lists before importing to Derby, use comparator
            dateTimeList.sort((o1, o2) -> o1.getId() - o2.getId());
            sensorList.sort((o1, o2) -> o1.getId() - o2.getId());
            countList.sort((o1, o2) -> o1.getId() - o2.getId());

            System.out.println("all the data loaded into the memory completes ...");

            importToDerby();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void insertDates(PreparedStatement psInsert) throws SQLException {
        for (DateTime date : dateTimeList) {
            psInsert.setInt(1, date.getId());
            psInsert.setString(2, date.getDesc());
            psInsert.setInt(3, date.getYear());
            psInsert.setInt(4, date.getMonth());
            psInsert.setInt(5, date.getDate());
            psInsert.setInt(6, date.getDay());
            psInsert.setInt(7, date.getTime());
            psInsert.executeUpdate();
        }
    }

    private void insertSensors(PreparedStatement psInsert) throws SQLException {
        for (Sensor sensor : sensorList) {
            psInsert.setInt(1, sensor.getId());
            psInsert.setString(2, sensor.getName());
            psInsert.executeUpdate();
        }
    }

    private void insertCounts(PreparedStatement psInsert) throws SQLException {
        for (Count count : countList) {
            psInsert.setInt(1, count.getId());
            psInsert.setInt(2, count.getHourlyCount());
            psInsert.setInt(3, count.getDateTimeId());
            psInsert.setInt(4, count.getSensorId());
            psInsert.executeUpdate();
        }
    }

    private void addToDateList(int dateId, String dateDesc, int year, int month, int date, int day, int time) {
        if (!dateIdSet.contains(dateId)) {
            dateIdSet.add(dateId);
            dateTimeList.add(new DateTime(dateId, dateDesc, year, month, date, day, time));
        }
    }

    private void addToCountList(int countId, int counts, int dateId, int sensorId) {
        countList.add(new Count(countId, counts, dateId, sensorId));
    }

    private void addToSensorList(int sensorId, String sensorName) {
        if (!sensorIdSet.contains(sensorId)) {
            sensorIdSet.add(sensorId);
            sensorList.add(new Sensor(sensorId, sensorName));
        }
    }

    private void createAndInsert(Connection conn, Statement state, String table, String[] sql) {
        String createSql = sql[0];
        String insertSql = sql[1];
        String querySql = sql[2];

        ResultSet result;
        System.out.println(" . . . . creating table " + table);
        try {
            state.execute(createSql);
            PreparedStatement psInsert = conn.prepareStatement(insertSql);
            System.out.printf("Inserted %s record%n", table);
            if (table.equals(sensorTable)) {
                insertSensors(psInsert);
//                result = state.executeQuery(querySql);
//                while (result.next()) {
//                    System.out.println(result.getInt(1) + " " + result.getString(2));
//                }
            } else if (table.equals(countTable)) {
                insertCounts(psInsert);
//                result = state.executeQuery(querySql);
//                while (result.next()) {
//                    System.out.println(result.getInt(1) + " " + result.getInt(2)
//                            + " " + result.getInt(3) + " " + result.getInt(4));
//                }
            } else if (table.equals(dateTable)) {
                insertDates(psInsert);
//                result = state.executeQuery(querySql);
//                while (result.next()) {
//                    System.out.println(result.getInt(1) + " " + result.getString(2) + " "
//                            + result.getInt(3) + " " + MONTHS[result.getInt(4)] + " "
//                            + result.getInt(5) + " " + DAYS[result.getInt(6)] + " " + result.getInt(7));
//                }
            }
            // Release the resources
            if (psInsert != null) {
                psInsert.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * important code reference:
     * https://stackoverflow.com/questions/18593019/if-exists-not-recognized-in-derby
     * partial code copied and modified from Derby installation directory Sample program WwdEmbedded.java
     * under directory: ./derby/demo/programs/workingwithderby/WwdEmbedded.java
     */
    private void importToDerby() {
        String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        String protocol = "jdbc:derby:";
        String dbName = "DerbyDB";
        String connectionUrl = protocol + dbName + ";create=true";

        int number = tables.length;
        String table;
        String[] sql;

        try (
                Connection conn = DriverManager.getConnection(connectionUrl);
                Statement state = conn.createStatement()
        ) {
            System.out.println("Connected to database " + dbName);
            // control transactions manually, autocommit is on by default in JDBC
            conn.setAutoCommit(false);
            // drop the table if exists
            for (int i = number - 1; i >= 0; --i) {
                table = tables[i];
                dropTable(table, conn, state);
            }
            System.out.println("Job begins: loading data into Derby ...");
            long start = System.currentTimeMillis();
            for (int i = 0; i < number; i++) {
                table = tables[i];
                sql = sqlList[i];
                createAndInsert(conn, state, table, sql);
            }
            long end = System.currentTimeMillis();
            long duration = end - start;
            System.out.printf("Job ends: loading data int Derby completes ... time taken %d millisecond = %.2f seconds%n", duration, duration / 1000f);
            //  commit the transaction: any changes will be persisted to the database now
            conn.commit();
            System.out.println("Committed the transaction");

            System.out.println("Closed connection");

            //  DATABASE SHUTDOWN SECTION
            //  In embedded mode, an application should shut down Derby
            //  Shutdown throws the XJ015 exception to confirm success
            boolean gotSqlExc = false;
            try {
                DriverManager.getConnection(protocol + ";shutdown=true");
            } catch (SQLException se) {
                String ex = "XJ015";
                if (ex.equals(se.getSQLState())) {
                    gotSqlExc = true;
                }
            }
            if (!gotSqlExc) {
                System.out.println("Database did not shut down normally");
            } else {
                System.out.println("Database shut down normally");
            }
            //  Beginning of the primary catch block: prints stack trace
        } catch (Throwable e) {
            //  Catch all exceptions and pass them to
            //  the Throwable.printStackTrace method
            System.out.println(" . . . exception thrown:");
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void init() {
        months = new String[]{"", "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"};
        days = new String[]{"", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};

        sensorTable = "SENSOR";
        String[] sensorSql = new String[]{
                String.format("create table %s (id int not null, name varchar(40) not null, primary key (id))", sensorTable),
                String.format("insert into %s (id, name) values (?, ?)", sensorTable),
                String.format("select id, name from %s", sensorTable)
        };

        dateTable = "DATETIME";
        String[] dateSql = new String[]{
                "create table " + dateTable + " (id int not null, desc_str varchar(24) not null,"
                        + " year_int int not null, month_int int not null, date_int int not null,"
                        + " day_int int not null, time_int int not null, primary key (id))",
                String.format("insert into %s (id, desc_str, year_int, month_int, date_int, day_int, time_int) values (?, ?, ?, ?, ?, ?, ?)", dateTable),
                String.format("select id, desc_str, year_int, month_int, date_int, day_int, time_int from %s", dateTable)
        };

        countTable = "COUNT";
        String[] countSql = new String[]{
                String.format("create table %s (id int not null, counts int not null, dateId int not null,"
                        + "sensorId int not null, primary key (id), foreign key (dateId) references %s (id),"
                        + "foreign key (sensorId) references %s (id))", countTable, dateTable, sensorTable),
                String.format("insert into %s (id, counts, dateId, sensorId) values (?, ?, ?, ?)", countTable),
                String.format("select id, counts, dateId, sensorId from %s", countTable)
        };

        tables = new String[]{dateTable, sensorTable, countTable};
        sqlList = new String[][]{dateSql, sensorSql, countSql};

        countList = new ArrayList<>();

        dateTimeList = new ArrayList<>();
        dateIdSet = new HashSet<>();

        sensorList = new ArrayList<>();
        sensorIdSet = new HashSet<>();
    }
}