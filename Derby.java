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
 * <p>
 * Loading data from CSV file then writing into Derby DB
 *
 * @author Kit T
 * @version 1.0
 * @since 12-April-2021
 */
public class Derby {
    private String[] months;
    private String[] days;
    /**
     * Count List to store all count records
     */
    private List<Count> countList;
    /**
     * Date Time List to store all datetime entities
     */
    private List<DateTime> dateTimeList;
    /**
     * Date Time ID set to eliminate duplicates
     */
    private Set<Integer> dateIdSet;
    /**
     * Sensor List to store all sensor entities
     */
    private List<Sensor> sensorList;
    /**
     * Sensor ID set to eliminate duplicates
     */
    private Set<Integer> sensorIdSet;
    private String dateTable;
    private String sensorTable;
    private String countTable;
    private String[] tables;
    /**
     * SQL statement array including create, insert, select
     */
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
        // ignore index 0
        for (int i = 1; i < len; i++) {
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
        // use table name to retrieve table metadata, if has next then indicates table exists
        if (resultSet.next()) {
            String sql = String.format("drop table %s", table);
            state.execute(sql);
            // use std_err as mentioned in the requirement
            System.err.println("Dropped table: " + table);
        }
    }

    /**
     * Derby Database loading program driver function, program entry point
     *
     * @param args command line arguments - an array of String arguments
     */
    public static void main(String[] args) {
        Derby derby = new Derby();
        // require file path argument from command line
        // verify arguments, if invalid then print error message and exit the program
        if (!derby.verifyArgs(args)) {
            // if file path is not provided then print error message and exit
            return;
        }
        final String path = args[0];
        System.err.println("Derby program starts ...");
        // initialise list containers for counts, datetime and sensor
        derby.init();
        // loading data from CSV file to memory then store into Derby Database
        derby.loadDate(path);
        System.err.println("Derby program finished ...");
    }

    /**
     * Verify if the command line arguments meet the requirement
     * standard format: java Derby datafile
     * args-1: path for datafile
     *
     * @param args an array of String arguments
     * @return true if requirements met, false otherwise
     */
    private boolean verifyArgs(String[] args) {
        // require file path argument from command line
        int required = 1;
        // if 1 condition met, a) no file path provided; b) file does not exist
        if (args == null || args.length < required || !(new File(args[0]).exists())) {
            System.err.println("insufficient number of arguments OR invalid arguments OR CSV file not exist");
            System.err.println("command to execute the program: java Derby datafile");
            System.err.println("example: java Derby file.csv");
            return false;
        }
        return true;
    }

    /**
     * important - code reference:
     * https://www.guru99.com/buffered-reader-in-java.html
     * Load data from csv file into memory, structure the data into 3 entities:
     * 1. count entity
     * 2. date time entity
     * 3. sensor entity
     *
     * @param path the directory path leads to the source file
     */
    private void loadDate(String path) {
        // auto closable without having to code finally code block
        // code reference: https://www.guru99.com/buffered-reader-in-java.html
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

                // extract the each attribute from the record, remove any space left-side and right-side
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

                // convert String into Int
                year = toInt(yearStr);
                date = toInt(dateStr);
                time = toInt(timeStr);
                // convert month and day into int, e.g. "September" -> 9, "Monday" -> 1
                month = getInt(monthStr, months);
                day = getInt(dayStr, days);

                // use year + month + date + time for datetime id
                // e.g. year - 2009, month - 10, date - 6, time - 9
                // datetime id will be 2009100609
                dateId = toInt(yearStr + (month < 10 ? "0" + month : month)
                        + (date < 10 ? "0" + date : date) + (time < 10 ? "0" + time : time));

                // add attributes of each entity into different list containers
                addToDateList(dateId, dateDesc, year, month, date, day, time);
                addToSensorList(sensorId, sensorName);
                addToCountList(countId, hourlyCounts, dateId, sensorId);
            }

            // sort all the lists in ascending order before importing to Derby, use comparator mechanism
            dateTimeList.sort((o1, o2) -> o1.getId() - o2.getId());
            sensorList.sort((o1, o2) -> o1.getId() - o2.getId());
            countList.sort((o1, o2) -> o1.getId() - o2.getId());
            // use std_err as required
            System.err.println("all the data from CSV file loaded into the memory completes ...");

            // import all list containers into Derby DB
            importToDerby();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Insert all the dates from date list to Derby DB
     *
     * @param psInsert prepared statement for insertion
     * @throws SQLException SQL exception
     */
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

    /**
     * Insert all the sensors from sensor list to Derby DB
     *
     * @param psInsert prepared statement for insertion
     * @throws SQLException SQL exception
     */
    private void insertSensors(PreparedStatement psInsert) throws SQLException {
        for (Sensor sensor : sensorList) {
            psInsert.setInt(1, sensor.getId());
            psInsert.setString(2, sensor.getName());
            psInsert.executeUpdate();
        }
    }

    /**
     * Insert all the counts from counts list to Derby DB
     *
     * @param psInsert prepared statement for insertion
     * @throws SQLException SQL exception
     */
    private void insertCounts(PreparedStatement psInsert) throws SQLException {
        for (Count count : countList) {
            psInsert.setInt(1, count.getId());
            psInsert.setInt(2, count.getHourlyCount());
            psInsert.setInt(3, count.getDateTimeId());
            psInsert.setInt(4, count.getSensorId());
            psInsert.executeUpdate();
        }
    }

    /**
     * Add all the datetime records from csv file to date list
     *
     * @param dateId   date id
     * @param dateDesc date time description
     * @param year     year int
     * @param month    month int
     * @param date     date int
     * @param day      day int
     * @param time     time int
     */
    private void addToDateList(int dateId, String dateDesc, int year, int month, int date, int day, int time) {
        // if data ID set does not contain dataId, then added to list
        // usage: eliminate the duplicates
        if (!dateIdSet.contains(dateId)) {
            dateIdSet.add(dateId);
            dateTimeList.add(new DateTime(dateId, dateDesc, year, month, date, day, time));
        }
    }

    /**
     * Add all the hourly counts records from csv file to counts list
     *
     * @param countId  count id
     * @param counts   hourly counts
     * @param dateId   datetime id
     * @param sensorId sensor id
     */
    private void addToCountList(int countId, int counts, int dateId, int sensorId) {
        countList.add(new Count(countId, counts, dateId, sensorId));
    }

    /**
     * Add all the sensors from csv file to sensor list
     *
     * @param sensorId   sensor id
     * @param sensorName sensor name
     */
    private void addToSensorList(int sensorId, String sensorName) {
        // if sensor ID set does not contain sensorId, then added to list
        // usage: eliminate the duplicates
        if (!sensorIdSet.contains(sensorId)) {
            sensorIdSet.add(sensorId);
            sensorList.add(new Sensor(sensorId, sensorName));
        }
    }

    /**
     * create 3 tables and insert all the data to the Database
     *
     * @param conn  DB connection
     * @param state statement
     * @param table table name
     * @param sql   SQL schema - create, insert, select
     */
    private void createAndInsert(Connection conn, Statement state, String table, String[] sql) {
        String createSql = sql[0];
        String insertSql = sql[1];
        String querySql = sql[2];

        ResultSet result;
        System.err.println("Sub task: creating table " + table);
        try {
            // execute sql to create the table
            state.execute(createSql);
            PreparedStatement psInsert = conn.prepareStatement(insertSql);
            System.err.printf("Sub task: insert records into %s%n", table);
            // insert data into table based on the table name
            if (table.equals(sensorTable)) {
                insertSensors(psInsert);
            } else if (table.equals(countTable)) {
                insertCounts(psInsert);
            } else if (table.equals(dateTable)) {
                insertDates(psInsert);
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

        // reference:
        // the code structure is copied and modified from Sample program WwdEmbedded.java
        // under directory: ./derby/demo/programs/workingwithderby/WwdEmbedded.java
        try (
                // auto closable, no need to include finally block to release resources
                Connection conn = DriverManager.getConnection(connectionUrl);
                Statement state = conn.createStatement()
        ) {
            // use std_err as required
            System.err.println("Connected to database " + dbName);
            // control transactions manually, autocommit is on by default in JDBC
            conn.setAutoCommit(false);
            // drop the table if exists
            for (int i = number - 1; i >= 0; --i) {
                table = tables[i];
                dropTable(table, conn, state);
            }
            // use std_err as required
            System.err.println("Job begins: loading data from memory into Derby ...");
            long start = System.currentTimeMillis();
            for (int i = 0; i < number; i++) {
                table = tables[i];
                sql = sqlList[i];
                createAndInsert(conn, state, table, sql);
            }
            long end = System.currentTimeMillis();
            // calculate the total time taken for loading data into Derby DB
            long duration = end - start;
            // use std_err as required
            System.err.printf("Job ends: loading data int Derby completes ... time taken %d millisecond = %.2f seconds%n", duration, duration / 1000f);
            //  commit the transaction: any changes will be persisted to the database now
            conn.commit();
            // use std_err as required
            System.err.println("Committed the transaction");
            System.err.println("Closed connection");

            // reference:
            // the code structure is copied and modified from Sample program WwdEmbedded.java
            // under directory: ./derby/demo/programs/workingwithderby/WwdEmbedded.java
            // DATABASE SHUTDOWN SECTION
            // In embedded mode, an application should shut down Derby
            // Shutdown throws the XJ015 exception to confirm success
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
                System.err.println("Database did not shut down normally");
            } else {
                System.err.println("Database shut down normally");
            }
            //  Beginning of the primary catch block: prints stack trace
        } catch (Throwable e) {
            //  Catch all exceptions and pass them to
            //  the Throwable.printStackTrace method
            System.err.println(" . . . exception thrown:");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    /**
     * initialise instance variables for Derby database creation and insertion
     * initialise list containers to load data from CSV file to memory container
     */
    private void init() {
        // initialise months and days array for later conversion, e.g. "January" -> 1, "Monday" -> 1
        // index 0 will be ignored, conversion starts from index 1
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
        // for count table, apply foreign key constraints reference from sensor and datetime table
        String[] countSql = new String[]{
                String.format("create table %s (id int not null, counts int not null, dateId int not null,"
                        + "sensorId int not null, primary key (id), foreign key (dateId) references %s (id),"
                        + "foreign key (sensorId) references %s (id))", countTable, dateTable, sensorTable),
                String.format("insert into %s (id, counts, dateId, sensorId) values (?, ?, ?, ?)", countTable),
                String.format("select id, counts, dateId, sensorId from %s", countTable)
        };
        tables = new String[]{dateTable, sensorTable, countTable};
        sqlList = new String[][]{dateSql, sensorSql, countSql};

        // initialise the list and set containers
        // usage for set is to eliminate data duplicates
        countList = new ArrayList<>();
        dateTimeList = new ArrayList<>();
        dateIdSet = new HashSet<>();
        sensorList = new ArrayList<>();
        sensorIdSet = new HashSet<>();
    }
}