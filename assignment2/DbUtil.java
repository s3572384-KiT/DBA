import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * DB utility class provides several helper functions for DB loading and DB querying
 * important - code reference:
 * https://www.tutorialspoint.com/java/java_documentation.htm
 *
 * @author Kit T
 * @version 1.0
 * @since 11-April-2021
 */
public class DbUtil {
    /**
     * verify if provided str can be converted into integer number
     *
     * @param str string needs to be verified
     * @return true if can not be covert to number, false can be converted to number
     */
    static boolean notNumber(String str) {
        if (str == null || str.length() <= 0) {
            return true;
        }
        char[] chars = str.toCharArray();
        // ascii code for '0' - '9' are 48 ~ 57
        // for each character, if it does not fall into [48, 57] range,
        // then this String str can not be converted into number
        for (char c : chars) {
            if (c < 48 || c > 57) {
                return true;
            }
        }
        return false;
    }

    /**
     * verify the existence of the file
     * important - code reference:
     * https://www.geeksforgeeks.org/file-exists-method-in-java-with-examples/
     *
     * @param name file name in String
     * @return true if can be covert to number, false otherwise
     */
    static boolean exists(String name) {
        if (null == name) {
            return false;
        }
        // check existence
        return new File(name).exists();
    }

    /**
     * convert string number "123" into 123, for instance
     *
     * @param str string needs conversion
     * @return number without double quotation
     */
    static int toInt(String str) {
        return Integer.parseInt(str);
    }

    /**
     * convert int data type variable into byte array, which then save as
     * raw data into the disk
     * important - code reference:
     * https://javadeveloperzone.com/java-basic/java-convert-int-to-byte-array/
     *
     * @param i int data type needs conversion to byte array
     * @return byte array
     */
    static byte[] intToBytes(final int i) {
        ByteBuffer buffer = ByteBuffer.allocate(Record.INT_SIZE);
        buffer.putInt(i);
        return buffer.array();
    }

    /**
     * convert byte array into 4 bytes int
     * important - code reference:
     * https://stackoverflow.com/questions/7619058/convert-a-byte-array-to-integer-in-java-and-vice-versa
     *
     * @param bytes  byte array needs conversion
     * @param offset starting point where conversion begins
     * @param len    the number of bytes from offset needs conversion
     * @return int number converted from bytes array
     */
    static int bytesToInt(byte[] bytes, int offset, int len) {
        return ByteBuffer.wrap(bytes, offset, len).getInt();
    }

    /**
     * convert byte array into String
     *
     * @param bytes   byte array needs conversion
     * @param offset  starting point where conversion begins
     * @param len     the number of bytes from offset needs conversion
     * @param charset charset used for decoding or conversion
     * @return new String converted from bytes array
     */
    static String bytesToStr(byte[] bytes, int offset, int len, Charset charset) {
        return new String(bytes, offset, len, charset).trim();
    }
}