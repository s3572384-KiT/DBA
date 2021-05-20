/**
 * task3: Java Heap File
 * entity Record
 * important - code reference:
 * https://www.tutorialspoint.com/java/java_documentation.htm
 *
 * @author Kit T
 * @version 1.0
 * @since 11-April-2021
 */
public class Record {

    /**
     * data type (attribute) size, int size is 4 bytes
     * maximum date time size is 22 bytes
     * maximum month size is 9 bytes, e.g. September
     * maximum day size is 9 bytes, e.g. Wednesday
     * maximum sensor name size is 38 bytes
     */
    static final int INT_SIZE = 4;
    static final int DATE_TIME_SIZE = 22;
    static final int MONTH_SIZE = 9;
    static final int DAY_SIZE = 9;
    static final int SENSOR_NAME_SIZE = 38;
    /**
     * SDT NAME is a new field: String(SensorId + DateTime)
     */
    static final int SDT_NAME_SIZE = INT_SIZE + DATE_TIME_SIZE;
    /**
     * offset for each attributes in the record
     */
    static final int ID_OFFSET = 0;
    static final int DATE_TIME_OFFSET = ID_OFFSET + INT_SIZE;
    static final int YEAR_OFFSET = DATE_TIME_OFFSET + DATE_TIME_SIZE;
    static final int MONTH_OFFSET = YEAR_OFFSET + INT_SIZE;
    static final int M_DATE_OFFSET = MONTH_OFFSET + MONTH_SIZE;
    static final int DAY_OFFSET = M_DATE_OFFSET + INT_SIZE;
    static final int TIME_OFFSET = DAY_OFFSET + DAY_SIZE;
    static final int SENSOR_ID_OFFSET = TIME_OFFSET + INT_SIZE;
    static final int SENSOR_NAME_OFFSET = SENSOR_ID_OFFSET + INT_SIZE;
    static final int HOURLY_COUNTS_OFFSET = SENSOR_NAME_OFFSET + SENSOR_NAME_SIZE;
    static final int SDT_NAME_OFFSET = HOURLY_COUNTS_OFFSET + INT_SIZE;
    /**
     * record size = id size + date time size + year size + month size + m date size + day size
     * + time size + sensor id size + sensor name size + hourly counts size + sdt name size
     * <p>
     * int_size * 6 including (id + year + m date + time + sensor id + hourly counts)
     */
    static int size = INT_SIZE * 6 + DATE_TIME_SIZE + MONTH_SIZE + DAY_SIZE + SENSOR_NAME_SIZE + SDT_NAME_SIZE;
}