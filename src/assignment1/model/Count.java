package assignment1.model;

/**
 * Count Entity
 * important - code reference:
 * https://www.tutorialspoint.com/java/java_documentation.htm
 *
 * @author Kit T
 * @version 1.0
 * @since 11-April-2021
 */
public class Count {
    private final int id;
    private final int hourlyCount;
    private final int dateTimeId;
    private final int sensorId;

    public Count(int id, int hourlyCount, int dateTimeId, int sensorId) {
        this.id = id;
        this.hourlyCount = hourlyCount;
        this.dateTimeId = dateTimeId;
        this.sensorId = sensorId;
    }

    public int getId() {
        return id;
    }

    public int getHourlyCount() {
        return hourlyCount;
    }

    public int getDateTimeId() {
        return dateTimeId;
    }

    public int getSensorId() {
        return sensorId;
    }

    @Override
    public String toString() {
        return String.format("id: %d, datetime: %d, sensor: %d, hourly counts: %d\n",
                id, dateTimeId, sensorId, hourlyCount);
    }
}