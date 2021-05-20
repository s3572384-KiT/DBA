import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Querying data from heap.size file
 * Searching given text from heap.size file then print out the result summary
 * important - code reference:
 * https://www.tutorialspoint.com/java/java_documentation.htm
 *
 * @author Kit T
 * @version 1.0
 * @since 11-April-2021
 */
public class DbQuery {

    static BPlusTree<Integer, Long> tree = new BPlusTree<>(100);

    static String heapPath = "heap.40960";

    /**
     * Verify if the command line arguments meet the requirement
     * <p>
     * command line arguments
     * standard format: java dbquery text pagesize
     * args-1: "109/24/2010 12:00:00 AM", args-2: a number represents page-size
     * example: java dbquery "109/24/2010 12:00:00 AM" 4096
     *
     * @param args an array of String arguments
     * @return true if requirements met, false otherwise
     */
    private static boolean verifyArgs(String[] args) {
        int required = 2;
        boolean valid = args != null && args.length >= required;

        if (valid) {
            if (DbUtil.notNumber(args[1])) {
                // if pageSize is not a number
                valid = false;
            } else {
                // check if heap file exist or not
                int pageSize = DbUtil.toInt(args[1]);
                String heapFile = "heap." + pageSize;
                valid = DbUtil.exists(heapFile);
            }
        }
        if (!valid) {
            System.err.println("insufficient number of arguments OR invalid arguments OR file not exist");
            System.err.println("command to execute the program: java dbquery text pagesize");
            System.err.println("example: java dbquery \"109/24/2010 12:00:00 AM\" 4096");
            return false;
        }
        return true;
    }

    /**
     * Perform search operation given the search text for SDT_NAME in heap file
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
     * @param text     search text from command line arguments
     * @param pageSize page size from command line arguments
     */
    private static void doSearch(String text, int pageSize) {
        String file = heapPath;
        Charset charset = StandardCharsets.UTF_8;
        try (InputStream inputStream = new FileInputStream(file)) {
            byte[] buffer = new byte[pageSize];
            byte[] record = new byte[Record.size];
            int size = record.length;
            int len, matchCount = 0;

            char tab = '\t';
            String sep = tab + "" + tab;
            // print header
            System.out.printf("ID%s%sDateTime%s%sYear%sMonth%sMDate%sDay%sTime%sSensorId%sSensorName%sHourly Counts%n",
                    sep, sep, sep, sep, sep, sep, sep, sep, sep, sep, sep);

            // header in the csv file
            int id = 0, year = 0, mDate = 0, time = 0, sensorId = 0, hourlyCounts = 0;
            String dateTime = null, month = null, day = null, sensorName = null, sdtName = null;
            long pos = 0;
            int cnt = 0;
            boolean go = false;
            // marking the start time when querying begins
            long start = System.currentTimeMillis();
            // each iteration, read exact 1 page-size into buffer
            while (inputStream.read(buffer) != -1) {
                len = buffer.length;
                int count = 0;
                while ((len -= size) > 0) {
                    // copy 1 record-size data from buffer to record-buffer
                    System.arraycopy(buffer, count++ * size, record, 0, size);
                    id = DbUtil.bytesToInt(record, 0, Record.INT_SIZE);
                    tree.insertOrUpdate(id,pos);
                    // if match found, then extract all attributes from record-buffer then print result
                    if (text.equalsIgnoreCase(String.valueOf(id))) {
                        ++matchCount;
                        dateTime = DbUtil.bytesToStr(record, Record.DATE_TIME_OFFSET, Record.DATE_TIME_SIZE, charset);
                        year = DbUtil.bytesToInt(record, Record.YEAR_OFFSET, Record.INT_SIZE);
                        month = DbUtil.bytesToStr(record, Record.MONTH_OFFSET, Record.MONTH_SIZE, charset);
                        mDate = DbUtil.bytesToInt(record, Record.M_DATE_OFFSET, Record.INT_SIZE);
                        day = DbUtil.bytesToStr(record, Record.DAY_OFFSET, Record.DAY_SIZE, charset);
                        time = DbUtil.bytesToInt(record, Record.TIME_OFFSET, Record.INT_SIZE);
                        sensorId = DbUtil.bytesToInt(record, Record.SENSOR_ID_OFFSET, Record.INT_SIZE);
                        sensorName = DbUtil.bytesToStr(record, Record.SENSOR_NAME_OFFSET, Record.SENSOR_NAME_SIZE, charset);
                        hourlyCounts = DbUtil.bytesToInt(record, Record.HOURLY_COUNTS_OFFSET, Record.INT_SIZE);
                        System.out.printf("%d%s%s%s%d%s%s%s%d%s%s%s%d%s%d%s%s%s%d\n",
                                id, sep, dateTime, sep, year, sep, month, sep, mDate, sep, day, sep, time, sep, sensorId, sep, sensorName, sep, hourlyCounts);

                    }
                }
                pos += len;
                ++cnt;
//                if( cnt > 10){
//                    System.out.println("stop");
//                    break;
//                }
            }
            // if no match record can be found given the search text
            if (matchCount == 0) {
                System.err.println("Sorry, no match can be found based on the given search text !!!");
            }
            // marking the start time when querying begins
            long end = System.currentTimeMillis();
            long duration = end - start;
            summaryReport(matchCount, duration);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    static void find(int key,int pageSize){
        long pos = tree.find(key);
        readRecordSkip(pos,pageSize,String.valueOf(key));
    }


    public static void readRecordSkip(long n,int pageSize,String text) {
        String file = heapPath;
        Charset charset = StandardCharsets.UTF_8;
        byte[] buffer = new byte[pageSize];
        byte[] record = new byte[Record.size];
        int id = 0, year , mDate , time , sensorId , hourlyCounts ;
        int size = record.length;
        int len, matchCount = 0;
        String sep = "\t" + "" + "\t";
        boolean go = false;
        String dateTime , month , day , sensorName;
        try(InputStream inputStream = new FileInputStream(file)){
            inputStream.skip(n);
            // marking the start time when querying begins
            long start = System.currentTimeMillis();
            // each iteration, read exact 1 page-size into buffer
            while (inputStream.read(buffer) != -1) {
                len = buffer.length;
                int count = 0;
                while ((len -= size) > 0) {
                    // copy 1 record-size data from buffer to record-buffer
                    System.arraycopy(buffer, count++ * size, record, 0, size);
                    // get the SDT_NAME from record-buffer
                    id = DbUtil.bytesToInt(record, 0, Record.INT_SIZE);
                    if(text.equalsIgnoreCase(String.valueOf(id))){
                        matchCount++;
                        dateTime = DbUtil.bytesToStr(record, Record.DATE_TIME_OFFSET, Record.DATE_TIME_SIZE, charset);
                        year = DbUtil.bytesToInt(record, Record.YEAR_OFFSET, Record.INT_SIZE);
                        month = DbUtil.bytesToStr(record, Record.MONTH_OFFSET, Record.MONTH_SIZE, charset);
                        mDate = DbUtil.bytesToInt(record, Record.M_DATE_OFFSET, Record.INT_SIZE);
                        day = DbUtil.bytesToStr(record, Record.DAY_OFFSET, Record.DAY_SIZE, charset);
                        time = DbUtil.bytesToInt(record, Record.TIME_OFFSET, Record.INT_SIZE);
                        sensorId = DbUtil.bytesToInt(record, Record.SENSOR_ID_OFFSET, Record.INT_SIZE);
                        sensorName = DbUtil.bytesToStr(record, Record.SENSOR_NAME_OFFSET, Record.SENSOR_NAME_SIZE, charset);
                        hourlyCounts = DbUtil.bytesToInt(record, Record.HOURLY_COUNTS_OFFSET, Record.INT_SIZE);
                        System.out.printf("%s%s%s%s%d%s%s%s%d%s%s%s%d%s%d%s%s%s%d\n",
                                id, sep, dateTime, sep, year, sep, month, sep, mDate, sep, day, sep, time, sep, sensorId, sep, sensorName, sep, hourlyCounts);
                    }
                }
//                System.out.println(id + "," + text);
                if(go){
                    break;
                }
            }

            if (matchCount == 0) {
                System.err.println("Sorry, no match can be found based on the given search text !!!");
            }
            // marking the start time when querying begins
            long end = System.currentTimeMillis();
            long duration = end - start;
            summaryReport(matchCount, duration);
        }catch (IOException e){
            e.printStackTrace();
        }

    }



    /**
     * Report summary when task finished
     *
     * @param matchCount the total number of matched records found in the heap file
     * @param duration   the total duration of time taken for the job in milliseconds
     */
    private static void summaryReport(int matchCount, long duration) {
        System.out.println("Summary Report: ");
        System.out.printf("\t1. the total number of matched record found: %d%n", matchCount);
        System.out.printf("\t2. the total amount time taken %d milliseconds = %.2f seconds for search operation%n", duration, duration / 1000f);
    }

    /**
     * DBQuery program driver function
     * 1. verify the command line arguments
     * 2. use search text to find all the matched records and print details
     * 3. keep tracking statistics during querying
     * 4. print summary
     * <p>
     * command line arguments
     * standard format: java dbquery text pagesize
     * args-1: "109/24/2010 12:00:00 AM", args-2: a number represents page-size
     * example: java dbquery "109/24/2010 12:00:00 AM" 4096
     *
     * @param args command line arguments - an array of String arguments
     */
    static public void main(String... args) {
        String searchText;
        int pageSize = 40960;
        // can be used for command line arguments
        searchText = "2887932";
        if(args.length > 0){
            heapPath = args[0];
            pageSize = Integer.parseInt(heapPath.substring(heapPath.indexOf("heap") + 5));
        }

        // extract the search text and page size information from command line
        System.out.println("DB query program starts ...");
        doSearch(searchText, pageSize);
        find(Integer.parseInt(searchText),pageSize);
        System.out.println("DB query program finished ...");
    }
}