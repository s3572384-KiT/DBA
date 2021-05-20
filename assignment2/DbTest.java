import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Kit T
 * @version 1.0
 * @since 11-April-2021
 */
class DbTest {
    public static void main(String[] args) {
//        String path = "Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv";
        String path = "/Users/nick/Desktop/assign/as/Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv";
        String line;
        final String comma = ",";

        // calculate the max length for String attributes: DATE_TIME, MONTH, DAY, SENSOR_NAME
        int dateTimeMax = 0;
        int monthMax = 0;
        int dayMax = 0;
        int sensorNameMax = 0;

        int count = 0;
        // header in the csv file
        int id, year, mDate, time, sensorId, hourlyCounts;
        String dateTime, month, day, sensorName;
        // new field SDT_NAME: Sensor_ID + DATE_TIME as String
        String sdtName;
        // column index in the csv file
        int idIdx = 0, dateTimeIdx = 1, yearIdx = 2, monthIdx = 3, mDateIdx = 4, dayIdx = 5;
        int timeIdx = 6, sensorIdIdx = 7, sensorNameIdx = 8, hourlyCountsIdx = 9;

        Map<String, Integer> sdtMap = new HashMap<>();

        //parsing a CSV file into BufferedReader class constructor
        try (
                // auto closable without having to code finally code block
                BufferedReader br = new BufferedReader(new FileReader(path));
        ) {
            String[] record;
            // when reading csv file, the 1st line is always header
            boolean isHeader = true;

            // marking start time when loading begins
            while ((line = br.readLine()) != null) {
                // skip header from writing
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                ++count;
                // split each record as a String array
                record = line.split(comma);
                dateTime = record[dateTimeIdx];
                month = record[monthIdx];
                day = record[dayIdx];
                sensorName = record[sensorNameIdx];
                sdtName = record[sensorIdIdx] + dateTime;

                dateTimeMax = Math.max(dateTimeMax, dateTime.length());
                monthMax = Math.max(monthMax, month.length());
                dayMax = Math.max(dayMax, day.length());
                sensorNameMax = Math.max(sensorNameMax, sensorName.length());

                if (!sdtMap.containsKey(sdtName)) {
                    sdtMap.put(sdtName, 1);
                } else {
                    sdtMap.put(sdtName, 1 + sdtMap.get(sdtName));
                }
            }
            System.out.println("the total number of records in the csv : " + count);

            System.out.println("the max length of attribute - datetime : " + dateTimeMax);
            System.out.println("the max length of attribute - month : " + monthMax);
            System.out.println("the max length of attribute - day : " + dayMax);
            System.out.println("the max length of attribute - sensor name : " + sensorNameMax);
            System.out.println("the total number of the unique sdt name : " + sdtMap.size());
            count = 0;
            for (Map.Entry<String, Integer> entry : sdtMap.entrySet()) {
                if (entry.getValue() > 1) {
                    if (++count > 10) {
                        break;
                    }
                    System.out.println(entry.getKey() + ": " + entry.getValue());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}