

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvUtils {

    private static final String COMMA = ",";

    /**
     * read date from csv file and store as Count Object
     * @param csvFile csv file
     * @param lines lines needs to be read,-1 means whole file lines
     * @return count object list
     */
    public static List<Count> readCountFromCsv(String csvFile,int lines){
        List<Count> countList = new ArrayList<>(4096000);
        // column index in the csv file
        int idIdx = 0, dateTimeIdx = 1, yearIdx = 2, monthIdx = 3, mDateIdx = 4, dayIdx = 5;
        int timeIdx = 6, sensorIdIdx = 7, sensorNameIdx = 8, hourlyCountsIdx = 9;

        //parsing a CSV file into BufferedReader class constructor
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            boolean isHeader = true;
            // marking start time when loading begins
            String line;
            while ((line = br.readLine()) != null) {
                // skip header from writing
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                Count countObj = new Count();
                // split each record as a String array
                String[] record = line.split(COMMA);
                countObj.id = Integer.parseInt(record[idIdx]);
                countObj.dateTime = record[dateTimeIdx];
                countObj.year = Integer.parseInt(record[yearIdx]);
                countObj.month = record[monthIdx];
                countObj.mDate = Integer.parseInt(record[mDateIdx]);
                countObj.day = record[dayIdx];
                countObj.time = Integer.parseInt(record[timeIdx]);
                countObj.sensorId = Integer.parseInt(record[sensorIdIdx]);
                countObj.sensorName = record[sensorNameIdx];
                countObj.hourlyCounts = Integer.parseInt(record[hourlyCountsIdx]);
                countList.add(countObj);
                if(lines != -1 && countList.size() >= lines){
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return countList;
    }

}
