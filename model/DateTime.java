package model;

/**
 * DateTime Entity
 * important - code reference:
 * https://www.tutorialspoint.com/java/java_documentation.htm
 *
 * @author Kit T
 * @version 1.0
 * @since 11-April-2021
 */
public class DateTime {
    private final int id;
    /**
     * DATE_TIME description
     */
    private final String desc;
    private final int year;
    private final int month;
    /**
     * M_DATE
     */
    private final int date;
    private final int time;
    private final int day;

    public DateTime(int id, String desc, int year, int month, int date, int day, int time) {
        this.id = id;
        this.desc = desc;
        this.year = year;
        this.month = month;
        this.date = date;
        this.day = day;
        this.time = time;
    }

    public int getId() {
        return id;
    }

    public String getDesc() {
        return desc;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getDate() {
        return date;
    }

    public int getTime() {
        return time;
    }

    public int getDay() {
        return day;
    }
}