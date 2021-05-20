import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Load data into heap.size file
 * Reading data from csv file then loaded into fixed page-size heap file
 * important - code reference:
 * https://www.tutorialspoint.com/java/java_documentation.htm
 *
 * @author Kit T
 * @version 1.0
 * @since 11-April-2021
 */
public class DbLoad {

    /**
     * Verify if the command line arguments meet the requirement
     * standard format: java dbload -p pagesize datafile
     * args-1: "-p", args-2: a number represents page-size, args-3: path for datafile
     *
     * @param args an array of String arguments
     * @return true if requirements met, false otherwise
     */
    private static boolean verifyArgs(String[] args) {
        if (args == null) {
            return false;
        }
        // len is number of arguments
        int len = args.length;
        // require 3 args: -p pagesize file
        int required = 3;
        String flag = "-p";
        int flagIdx = 0, sizeIdx = 1, fileIdx = 2;

        // if any condition is met, 1. not enough ars or 2. flag is not "-p" or 3. not number or 4. file not exist
        if (len < required || !flag.equals(args[flagIdx]) || DbUtil.notNumber(args[sizeIdx]) || !DbUtil.exists(args[fileIdx])) {
            System.err.println("insufficient number of arguments OR invalid arguments OR file not exist");
            System.err.println("command to execute the program: java dbload -p pagesize datafile");
            System.err.println("example: java dbload -p 4096 file.csv");
            return false;
        }
        return true;
    }

    /**
     * Load each attribute in the record into fix-size block of the buffer
     * important - code reference:
     * https://www.baeldung.com/java-string-to-byte-array
     *
     * @param record an array of String for all attributes of the record
     * @param buffer byte buffer for data written
     */
    static private void recordToBuffer(String[] record, ByteBuffer buffer) {
        Charset charset = StandardCharsets.UTF_8;

        // header in the csv file
        int id, year, mDate, time, sensorId, hourlyCounts;
        String dateTime, month, day, sensorName;
        // new field SDT_NAME: Sensor_ID + DATE_TIME as String
        String sdtName;
        // column index in the csv file
        int idIdx = 0, dateTimeIdx = 1, yearIdx = 2, monthIdx = 3, mDateIdx = 4, dayIdx = 5;
        int timeIdx = 6, sensorIdIdx = 7, sensorNameIdx = 8, hourlyCountsIdx = 9;
        // toInt converts string-number to integer
        id = DbUtil.toInt(record[idIdx]);
        dateTime = record[dateTimeIdx];
        year = DbUtil.toInt(record[yearIdx]);
        month = record[monthIdx];
        mDate = DbUtil.toInt(record[mDateIdx]);
        day = record[dayIdx];
        time = DbUtil.toInt(record[timeIdx]);
        sensorId = DbUtil.toInt(record[sensorIdIdx]);
        sensorName = record[sensorNameIdx];
        hourlyCounts = DbUtil.toInt(record[hourlyCountsIdx]);
        sdtName = record[sensorIdIdx] + dateTime;

        // put id as a byte array into buffer, 0 means start from beginning, total length is 4 bytes
        buffer.put(DbUtil.intToBytes(id), 0, Record.INT_SIZE);
        buffer.put(dateTime.getBytes(charset), 0, dateTime.length());

        // size for date_time may be vary, reset any remaining slot allocated to DATE_TIME field to 0
        clearRemainingSlot(buffer, dateTime.length(), Record.DATE_TIME_SIZE);

        buffer.put(DbUtil.intToBytes(year), 0, Record.INT_SIZE);
        buffer.put(month.getBytes(charset), 0, month.length());

        // size for month may be vary, reset any remaining slot allocated to MONTH field to 0
        clearRemainingSlot(buffer, month.length(), Record.MONTH_SIZE);

        buffer.put(DbUtil.intToBytes(mDate), 0, Record.INT_SIZE);
        buffer.put(day.getBytes(charset), 0, day.length());

        // size for day may be vary, reset any remaining slot allocated to DAY field to 0
        clearRemainingSlot(buffer, day.length(), Record.DAY_SIZE);

        buffer.put(DbUtil.intToBytes(time), 0, Record.INT_SIZE);
        buffer.put(DbUtil.intToBytes(sensorId), 0, Record.INT_SIZE);
        buffer.put(sensorName.getBytes(charset), 0, sensorName.length());

        // size for sensor_name may be vary, reset any remaining slot allocated to SENSOR_NAME field to 0
        clearRemainingSlot(buffer, sensorName.length(), Record.SENSOR_NAME_SIZE);

        buffer.put(DbUtil.intToBytes(hourlyCounts), 0, Record.INT_SIZE);
        buffer.put(sdtName.getBytes(charset), 0, sdtName.length());
        // size for sdt_name may be vary, reset any remaining slot allocated to SDT_NAME field to 0
        clearRemainingSlot(buffer, sdtName.length(), Record.SDT_NAME_SIZE);
    }

    /**
     * Reset the remaining allocated slot with ZERO 0, reason is for any given String attribute,
     * it may only fill partial allocated slot, to keep data clean in the buffer, reset the remaining
     * slot is necessary
     *
     * @param buffer    byte buffer where data is loading to
     * @param strSize   the size of the given String attribute
     * @param totalSize the total allocated size for any given String attribute
     */
    private static void clearRemainingSlot(ByteBuffer buffer, int strSize, int totalSize) {
        // if total size > str size, indicating there is still remaining slots not filled
        if (totalSize > strSize) {
            // calculate the number of remaining slots
            int len = totalSize - strSize;
            // set up zeros to clear the page size buffer, by default value in array is 0
            // e.g. { 0, 0, 0, ... 0 }
            byte[] zeros = new byte[len];
            buffer.put(zeros, 0, len);
        }
    }

    /**
     * return current time in milliseconds
     *
     * @return time in milliseconds
     */
    private static long getCurTime() {
        return System.currentTimeMillis();
    }

    /**
     * Loading data from source file to heap.size file
     * important - code reference:
     * https://www.javatpoint.com/how-to-read-csv-file-in-java
     * https://mkyong.com/java/how-to-read-and-parse-csv-file-in-java/
     * https://stackabuse.com/reading-and-writing-csvs-in-java/
     * https://stackoverflow.com/questions/579600/how-to-put-the-content-of-a-bytebuffer-into-an-outputstream
     * http://www.java2s.com/Tutorial/Java/0180__File/WritingandAppendingaByteBuffertoaFile.htm
     * https://www.codota.com/code/java/methods/java.nio.Buffer/reset
     * https://www.codota.com/code/java/methods/java.nio.ByteBuffer/remaining
     * https://stackoverflow.com/questions/12108796/java-bytebuffer-filling-up-completely
     *
     * @param pageSize  size for each page
     * @param inputFile data source file
     */
    private static void dataLoading(int pageSize, String inputFile) {
        String line;
        final String comma = ",";
        // count for number of records written per page
        int count = 0;
        // use to keep tracking the total number records written
        int recordCount = 0;
        // use to keep tracking the total number of pages
        int pageCount = 0;
        // use size to check if there is still enough space for 1 record in the buffer
        int size;
        // output file name, e.g. heap.4096
        String outFile = String.format("heap.%d", pageSize);

        // code references mentioned above
        //parsing a CSV file into BufferedReader class constructor
        try (
                // auto closable without having to code finally code block
                BufferedReader br = new BufferedReader(new FileReader(inputFile));
                FileOutputStream outStream = new FileOutputStream(outFile);
                WritableByteChannel channel = Channels.newChannel(outStream)
        ) {
            String[] record;
            // allocate a fix-sized page size buffer, by default value for byte in array is 0
            // e.g. { 0, 0, 0, ... 0 }
            ByteBuffer buffer = ByteBuffer.allocate(pageSize);
            // use size to measure how many records have written to the buffer
            size = pageSize;
            // false when page buffer still has space for record size, true when page is insufficient
            boolean insufficient = false;
            // when reading csv file, the 1st line is always header
            boolean isHeader = true;

            // marking start time when loading begins
            long start = getCurTime();
            while ((line = br.readLine()) != null) {
                // skip header from writing
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                // split each record as a String array
                record = line.split(comma);
                // deduct the record-size to indicate 1 block record-size space is taken
                // when size is less than record-size, indicating not enough space of current page for record
                if ((size -= Record.size) < Record.size) {
                    insufficient = true;
                }

                // write each record into buffer
                recordToBuffer(record, buffer);
                // count the total number of records written for current page
                ++count;

                // if not enough page space for any more record, accumulate count to Record Count
                // reset 1. insufficient to false 2. size to page-size 3. count to 0 for new page
                if (insufficient) {
                    ++pageCount;
                    recordCount += count;
                    insufficient = false;
                    size = pageSize;
                    count = 0;
                    // clear buffer does not clear the content of the buffer,
                    // but reset position of buffer from end to 0 for writing the content of buffer from beginning
                    buffer.clear();
                    channel.write(buffer);
                    // after finishing writing, reset position of buffer from end to 0 again for new page
                    // writing record into the buffer from starting position 0
                    buffer.clear();
                }
            }
            // marking end time when loading finished
            long end = getCurTime();
            long duration = end - start;
            // report summary when task finished
            summaryReport(recordCount, pageCount, duration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Report summary when task finished
     *
     * @param recordCount the total number of records loaded to the heap file
     * @param pageCount   the total number of pages written during loading
     * @param duration    the total duration of time taken for the job in milliseconds
     */
    private static void summaryReport(int recordCount, int pageCount, long duration) {
        System.out.println("Summary Report: ");
        System.out.printf("\t1. total number of records loaded to the file: %d%n", recordCount);
        System.out.printf("\t2. total number of pages used: %d%n", pageCount);
        System.out.printf("\t3. total number of milliseconds to create the heap file: %d milliseconds = %.2f seconds%n", duration, duration / 1000f);
    }

    /**
     * DBLoad program driver function
     * 1. verify the command line arguments
     * 2. perform data loading
     * 3. keep tracking statistics during loading
     * 4. print summary
     * <p>
     * command line arguments
     * standard format: java dbload -p pagesize datafile
     * args-1: "-p", args-2: a number represents page-size, args-3: path for datafile
     *
     * @param args command line arguments - an array of String arguments
     */
    static public void main(String... args) {
        // verify arguments
        // if invalid, then print error message and exit program when invalid args provided
        int pageSize = 40960;
        String file = "count.csv";
//        if (!verifyArgs(args)) {
//            return;
//        }
        // extract the page size and file name information from command line

        // System.out.println("PAGE SIZE = " + pageSize + " - FILE NAME = " + file);
        System.out.println("DB loading program starts ...");
        dataLoading(pageSize, file);
        System.out.println("DB loading program finished ...");
    }
}