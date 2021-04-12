import model.Count;
import model.DateTime;
import model.Sensor;

import java.io.BufferedReader;
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

    private final static String[] MONTHS;
    private final static String[] DAYS;

    private final static List<Count> COUNT_LIST;

    private final static List<DateTime> DATE_TIME_LIST;
    private final static Set<Integer> DATE_ID_SET;

    private final static List<Sensor> SENSOR_LIST;
    private final static Set<Integer> SENSOR_ID_SET;

    private final static String DATE_TABLE;
    private final static String[] DATE_SQL;

    private final static String SENSOR_TABLE;
    private final static String[] SENSOR_SQL;

    private final static String COUNT_TABLE;
    private final static String[] COUNT_SQL;

    private final static String[] TABLES;
    private final static String[][] SQL_LIST;

    static {
        MONTHS = new String[]{"", "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"};
        DAYS = new String[]{"", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};

        SENSOR_TABLE = "SENSOR";
        SENSOR_SQL = new String[]{
                String.format("create table %s (id int not null, name varchar(40) not null, primary key (id))", SENSOR_TABLE),
                String.format("insert into %s (id, name) values (?, ?)", SENSOR_TABLE),
                String.format("select id, name from %s", SENSOR_TABLE)
        };

        DATE_TABLE = "DATETIME";
        DATE_SQL = new String[]{
                "create table " + DATE_TABLE + " (id int not null, desc_str varchar(24) not null,"
                        + " year_int int not null, month_int int not null, date_int int not null,"
                        + " day_int int not null, time_int int not null, primary key (id))",
                String.format("insert into %s (id, desc_str, year_int, month_int, date_int, day_int, time_int) values (?, ?, ?, ?, ?, ?, ?)", DATE_TABLE),
                String.format("select id, desc_str, year_int, month_int, date_int, day_int, time_int from %s", DATE_TABLE)
        };

        COUNT_TABLE = "COUNT";
        COUNT_SQL = new String[]{
                String.format("create table %s (id int not null, counts int not null, dateId int not null,"
                        + "sensorId int not null, primary key (id), foreign key (dateId) references %s (id),"
                        + "foreign key (sensorId) references %s (id))", COUNT_TABLE, DATE_TABLE, SENSOR_TABLE),
                String.format("insert into %s (id, counts, dateId, sensorId) values (?, ?, ?, ?)", COUNT_TABLE),
                String.format("select id, counts, dateId, sensorId from %s", COUNT_TABLE)
        };

        TABLES = new String[]{DATE_TABLE, SENSOR_TABLE, COUNT_TABLE};
        SQL_LIST = new String[][]{DATE_SQL, SENSOR_SQL, COUNT_SQL};

        COUNT_LIST = new ArrayList<>();

        DATE_TIME_LIST = new ArrayList<>();
        DATE_ID_SET = new HashSet<>();

        SENSOR_LIST = new ArrayList<>();
        SENSOR_ID_SET = new HashSet<>();
    }

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

    private static int toInt(String str) {
        return Integer.parseInt(str);
    }

    /**
     * Load data from csv file into memory, structure the data into 3 entities:
     * 1. count entity
     * 2. date time entity
     * 3. sensor entity
     *
     * @param path the directory path leads to the source file
     */
    private static void loadDate(String path) {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            final String COMMA = ",";
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
                data = record.split(COMMA);

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
                month = getInt(monthStr, MONTHS);
                day = getInt(dayStr, DAYS);

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
            DATE_TIME_LIST.sort((o1, o2) -> o1.getId() - o2.getId());
            SENSOR_LIST.sort((o1, o2) -> o1.getId() - o2.getId());
            COUNT_LIST.sort((o1, o2) -> o1.getId() - o2.getId());

            System.out.println("all the data loaded into the memory completes ...");

            importToDerby();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void insertDates(PreparedStatement psInsert) throws SQLException {
        for (DateTime date : DATE_TIME_LIST) {
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

    private static void insertSensors(PreparedStatement psInsert) throws SQLException {
        for (Sensor sensor : SENSOR_LIST) {
            psInsert.setInt(1, sensor.getId());
            psInsert.setString(2, sensor.getName());
            psInsert.executeUpdate();
        }
    }

    private static void insertCounts(PreparedStatement psInsert) throws SQLException {
        for (Count count : COUNT_LIST) {
            psInsert.setInt(1, count.getId());
            psInsert.setInt(2, count.getHourlyCount());
            psInsert.setInt(3, count.getDateTimeId());
            psInsert.setInt(4, count.getSensorId());
            psInsert.executeUpdate();
        }
    }

    private static void addToDateList(int dateId, String dateDesc, int year, int month, int date, int day, int time) {
        if (!DATE_ID_SET.contains(dateId)) {
            DATE_ID_SET.add(dateId);
            DATE_TIME_LIST.add(new DateTime(dateId, dateDesc, year, month, date, day, time));
        }
    }

    private static void addToCountList(int countId, int counts, int dateId, int sensorId) {
        COUNT_LIST.add(new Count(countId, counts, dateId, sensorId));
    }

    private static void addToSensorList(int sensorId, String sensorName) {
        if (!SENSOR_ID_SET.contains(sensorId)) {
            SENSOR_ID_SET.add(sensorId);
            SENSOR_LIST.add(new Sensor(sensorId, sensorName));
        }
    }

    private static void createAndInsert(Connection conn, Statement state, String table, String[] sql) {
        String createSql = sql[0];
        String insertSql = sql[1];
        String querySql = sql[2];

        ResultSet result;
        System.out.println(" . . . . creating table " + table);
        try {
            state.execute(createSql);
            PreparedStatement psInsert = conn.prepareStatement(insertSql);
            System.out.printf("Inserted %s record%n", table);
            if (table.equals(SENSOR_TABLE)) {
                insertSensors(psInsert);
//                result = state.executeQuery(querySql);
//                while (result.next()) {
//                    System.out.println(result.getInt(1) + " " + result.getString(2));
//                }
            } else if (table.equals(COUNT_TABLE)) {
                insertCounts(psInsert);
//                result = state.executeQuery(querySql);
//                while (result.next()) {
//                    System.out.println(result.getInt(1) + " " + result.getInt(2)
//                            + " " + result.getInt(3) + " " + result.getInt(4));
//                }
            } else if (table.equals(DATE_TABLE)) {
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
     */
    private static void importToDerby() {
        String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        String protocol = "jdbc:derby:";
        String dbName = "DerbyDB";
        String connectionUrl = protocol + dbName + ";create=true";

        int number = TABLES.length;
        String table;
        String[] sql;

        try (
                Connection conn = DriverManager.getConnection(connectionUrl);
                Statement state = conn.createStatement();
        ) {
            System.out.println("Connected to database " + dbName);
            // control transactions manually, autocommit is on by default in JDBC
            conn.setAutoCommit(false);
            // drop the table if exists
            for (int i = number - 1; i >= 0; --i) {
                table = TABLES[i];
                dropTable(table, conn, state);
            }
            System.out.println("loading data into Derby begins ...");
            long start = System.currentTimeMillis();
            for (int i = 0; i < number; i++) {
                table = TABLES[i];
                sql = SQL_LIST[i];
                createAndInsert(conn, state, table, sql);
            }
            long end = System.currentTimeMillis();
            long duration = end - start;
            System.out.printf("loading data int Derby completes ... time taken %d millisecond = %.2f seconds", duration, duration / 1000f);
            // drop the table if exists
//            for (int i = number - 1; i >= 0; --i) {
//                table = TABLES[i];
//                dropTable(table, conn, state);
//            }
//            commit the transaction. Any changes will be persisted to the database now.
            conn.commit();
            System.out.println("Committed the transaction");

            System.out.println("Closed connection");

            //   ## DATABASE SHUTDOWN SECTION ##
            // In embedded mode, an application should shut down Derby.
//            Shutdown throws the XJ015 exception to confirm success.
            boolean gotSQLExc = false;
            try {
                DriverManager.getConnection("jdbc:derby:;shutdown=true");
            } catch (SQLException se) {
                if ("XJ015".equals(se.getSQLState())) {
                    gotSQLExc = true;
                }
            }
            if (!gotSQLExc) {
                System.out.println("Database did not shut down normally");
            } else {
                System.out.println("Database shut down normally");
            }
        } catch (Throwable e) {
            /*       Catch all exceptions and pass them to
             *       the Throwable.printStackTrace method  */
            System.out.println(" . . . exception thrown:");
            e.printStackTrace(System.out);
            System.exit(1);
        }
        System.out.println("Getting Started With Derby JDBC program ending.");
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
            String sql = "drop table " + table;
            state.execute(sql);
            System.out.println("Dropped table: " + table);
        }
    }

    static public void main(String... args) {
        final String path = "../count.csv";
        loadDate(path);
    }
}