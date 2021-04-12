import model.Count;
import model.DateTime;
import model.Sensor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * important - code reference:
 * https://www.tutorialspoint.com/java/java_documentation.htm
 *
 * @author Kit T
 * @version 1.0
 * @since 11-April-2021
 */
public class Derby {

    private final static String[] MONTHS;
    private final static String[] DAYS;
    private final static Map<String, Integer> MONTHS_MAP;
    private final static Map<String, Integer> DAYS_MAP;

    private final static List<Count> COUNT_LIST;

    private final static List<DateTime> DATE_TIME_LIST;
    private final static Set<Integer> DATE_ID_SET;

    private final static List<Sensor> SENSOR_LIST;
    private final static Set<Integer> SENSOR_ID_SET;
    private final static Map<Integer, String> SENSOR_MAP;

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
                String.format("create table %s (id int not null, name varchar(40) not null, primary key(id))", SENSOR_TABLE),
                String.format("insert into %s (id, name) values (?, ?)", SENSOR_TABLE),
                String.format("select id, name from %s", SENSOR_TABLE)
        };

        DATE_TABLE = "DATETIME";
        DATE_SQL = new String[]{
                "create table " + DATE_TABLE + " (id int not null, desc_str varchar(24) not null,"
                        + " year_int int not null, month_int int not null, date_int int not null,"
                        + " day_int int not null, time_int int not null, primary key(id))",
                String.format("insert into %s (id, desc_str, year_int, month_int, date_int, day_int, time_int) values (?, ?, ?, ?, ?, ?, ?)", DATE_TABLE),
                String.format("select id, desc_str, year_int, month_int, date_int, day_int, time_int from %s", DATE_TABLE)
        };

        COUNT_TABLE = "COUNT";
        COUNT_SQL = new String[]{
                String.format("create table %s (id int not null, counts int not null, dateId int not null,"
                        + "sensorId int not null, primary key(id), constraint fk_dateId foreign key (dateId) references %s(id),"
                        + "constraint fk_sensorId foreign key (sensorId) references %s(id)", COUNT_TABLE, DATE_TABLE, SENSOR_TABLE),
                String.format("insert into %s (id, counts, dateId, sensorId) values (?, ?, ?, ?)", COUNT_TABLE),
                String.format("select id, counts, dateId, sensorId from %s", COUNT_TABLE)
        };

        TABLES = new String[]{DATE_TABLE, SENSOR_TABLE, COUNT_TABLE};
        SQL_LIST = new String[][]{DATE_SQL, SENSOR_SQL, COUNT_SQL};

        MONTHS_MAP = new HashMap<>();
        for (int i = 1; i < MONTHS.length; i++) {
            MONTHS_MAP.put(MONTHS[i].toLowerCase(), i);
        }
        DAYS_MAP = new HashMap<>();
        for (int i = 1; i < DAYS.length; i++) {
            DAYS_MAP.put(DAYS[i].toLowerCase(), i);
        }

        COUNT_LIST = new ArrayList<>();

        DATE_TIME_LIST = new ArrayList<>();
        DATE_ID_SET = new HashSet<>();

        SENSOR_LIST = new ArrayList<>();
        SENSOR_ID_SET = new HashSet<>();
        SENSOR_MAP = new HashMap<>();
    }

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
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                data = record.split(COMMA);

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

                if (!MONTHS_MAP.containsKey(monthStr.toLowerCase())) {
                    System.out.println("month does not match !!!");
                    return;
                }
                if (!DAYS_MAP.containsKey(dayStr.toLowerCase())) {
                    System.out.println("day does not match !!!");
                    return;
                }
                month = MONTHS_MAP.get(monthStr.toLowerCase());
                day = DAYS_MAP.get(dayStr.toLowerCase());

                dateId = toInt(yearStr
                        + (month < 10 ? "0" + month : month)
                        + (date < 10 ? "0" + date : date)
                        + (time < 10 ? "0" + time : time));

                addToDateList(dateId, dateDesc, year, month, date, day, time);
                addToSensorList(sensorId, sensorName);
                addToCountList(countId, hourlyCounts, dateId, sensorId);
            }

            DATE_TIME_LIST.sort((o1, o2) -> o1.getId() - o2.getId());
            SENSOR_LIST.sort((o1, o2) -> o1.getId() - o2.getId());
            COUNT_LIST.sort((o1, o2) -> o1.getId() - o2.getId());
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

    private static void createAndInsert(String table, String[] sql, Connection conn, Statement state) {
        String createSql = sql[0];
        String insertSql = sql[1];
        String querySql = sql[2];

        ResultSet result;
        System.out.println(" . . . . creating table " + table);
        try {
            state.execute(createSql);
            PreparedStatement psInsert = conn.prepareStatement(insertSql);
            if (table.equals(SENSOR_TABLE)) {
                insertSensors(psInsert);
                System.out.println("Inserted sensor record");

                result = state.executeQuery(querySql);
                while (result.next()) {
                    System.out.println(result.getInt(1) + " " + result.getString(2));
                }
            } else if (table.equals(COUNT_TABLE)) {
                insertCounts(psInsert);
                System.out.println("Inserted count record");

                result = state.executeQuery(querySql);
                while (result.next()) {
                    System.out.println(result.getInt(1) + " " + result.getInt(2)
                            + " " + result.getInt(3) + " " + result.getInt(4));
                }
            } else if (table.equals(DATE_TABLE)) {
                insertDates(psInsert);
                System.out.println("Inserted date record");

                result = state.executeQuery(querySql);
                while (result.next()) {
                    System.out.println(result.getInt(1) + " " + result.getString(2) + " "
                            + result.getInt(3) + " " + MONTHS[result.getInt(4)] + " "
                            + result.getInt(5) + " " + DAYS[result.getInt(6)] + " " + result.getInt(7));
                }
            }
            // Release the resources (clean up )
            if (psInsert != null) {
                psInsert.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

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
            // want to control transactions manually. Autocommit is on by default in JDBC.
            conn.setAutoCommit(false);

            for (int i = 0; i < number; i++) {
                table = TABLES[i];
                sql = SQL_LIST[i];

                createAndInsert(table, sql, conn, state);

                // delete the table
                state.execute("drop table " + table);
                System.out.println("Dropped table " + table);

                /*
                   We commit the transaction. Any changes will be persisted to
                   the database now.
                 */
                conn.commit();
                System.out.println("Committed the transaction");
            }

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

    static public void main(String... args) {
        final String path = "/root/rmit/Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv";
        loadDate(path);
    }

    public static void main1(String[] args) throws IOException {
        final String COMMA = ",";
        final String path = "/root/rmit/Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv";

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            String[] values;
            int sensorId, dateId, statId, counts, year, month, date, day, time;
            String desc;
            String sensorName;
            String regex = "\\d+/\\d+/\\d+\\s\\d+:\\d+:\\d+";
            boolean isHeader = true;
            while ((line = br.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                values = line.split(COMMA);

                statId = toInt(values[0].trim());
                desc = values[1].trim();
                year = toInt(values[2].trim());
                month = MONTHS_MAP.get(values[3].trim());
                date = toInt(values[4].trim());
                day = DAYS_MAP.get(values[5].trim());
                time = toInt(values[6].trim());
                counts = toInt(values[9].trim());
                sensorId = toInt(values[7].trim());
                sensorName = values[8].trim();

//                dateId = calcDateId(time, day, date, month, year);

//                set1.add(values[0].trim());
//                set2.add(values[2].trim() + values[3].trim() + values[4].trim() + values[5].trim() + values[6].trim());
//                set3.add(values[7].trim());

//                System.out.println("time " + time + "  day: " + day + "  month:  " + month + "  year: " + year);
//                System.out.println(dateId);

//                if (!dateIdSet.contains(dateId)) {
//                    dateIdSet.add(dateId);
//                    dateTimeList.add(new DateTime(dateId, desc, year, month, date, day, time));
//                }
//
//                if (!sensorList.contains(sensorId)) {
//                    sensorList.push(new Sensor(sensorId, sensorName));
//                }


                if (!SENSOR_MAP.containsKey(sensorId)) {
                    SENSOR_MAP.put(sensorId, sensorName);
                    SENSOR_LIST.add(new Sensor(sensorId, sensorName));
                } else {
                    if (!sensorName.equals(SENSOR_MAP.get(sensorId))) {
                        System.out.println("found same sensor id but different name: " + sensorId);
                        System.out.println(sensorName);
                        System.out.println(SENSOR_MAP.get(sensorId));
                    }
                }

//                if (!sensorIdSet.contains(sensorId)) {
//                    sensorIdSet.add(sensorId);
//                    sensors.add(new Sensor(sensorId, sensorName));
//                }

//                statList.add(new CountStat(statId, counts, dateId, sensorId));
            }
            System.out.println("sensor id set: " + SENSOR_ID_SET.size() + "  sensor size: " + SENSOR_LIST.size());
//            System.out.println("stat size: " + statList.size());
//            System.out.println("date time id set : " + dateIdSet.size() + "   datetime size:" + dateTimeList.size());

//            System.out.println("set1 : " + set1.size());
//            System.out.println("set2 : " + set2.size());
//            System.out.println("set3 : " + set3.size());

            SENSOR_LIST.sort(Comparator.comparingInt(Sensor::getId));
            for (Sensor sensor : SENSOR_LIST) {
                System.out.println(sensor);
            }

            System.out.println("===================");

//            sensorList.sortAscend();
//            System.out.println(sensorList);

//            if (sensorList.size() != sensors.size()) {
//                System.out.println("fail ...");
//                return;
//            }
//            int size = sensorList.size();
//            for (int i = 0; i < size; i++) {
//                if (!sensorList.get(i).equals(sensors.get(i))) {
//                    System.out.println("not equal ...");
//                    return;
//                }
//            }
//            System.out.println("ok");
        }
    }

    private static int toInt(String str) {
        return Integer.parseInt(str);
    }
}