import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Read pedestrian data from csv file, and push the lines into a blocking queue.
 * Block and wait when the queue reaches its maximum length.
 * <p>
 *     new Thread(new CsvReader()).start();
 * </p>
 */
public class CsvReader implements Runnable {

    /**
     * The flag which indicates whether it reaches the end of the file
     * default value is false
     */
    private boolean eof;

    /**
     * The queue stores the data read from file.
     * The reading will be blocked when the queue reaches its maximum length,
     * and it will continue reading if data is removed from the queue and there is space for new data.
     */
    private BlockingQueue<String> buffer;

    /**
     * The reading file path
     */
    private String sourceFilePath;

    public CsvReader(String sourceFilePath) {
        this.eof = false;
        this.sourceFilePath = sourceFilePath;
        // initialize the buffer
        this.buffer = new ArrayBlockingQueue<>(50);
    }

    @Override
    public void run() {
        int lineNumber = 0;
        try {
            /* read the source file and create memory mapping */
            RandomAccessFile memoryMappedFile = new RandomAccessFile(sourceFilePath,"r");
            int size = (int) memoryMappedFile.length();
            MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY,0,size);

            // start time of reading source file
            long start = System.currentTimeMillis();

            // assume that the length of rows is 200 bytes
            final int extra = 200;
            int count = extra;
            byte[] buf = new byte[count];

            int j=0;
            char ch ='\0';
            boolean flag = false;
            // traverse every byte of the file
            while(out.remaining() > 0) {
                byte by = out.get();
                ch =(char) by;
                switch(ch) {
                    case '\n':
                    case '\r':
                        flag = true;
                        break;
                    default:
                        buf[j] = by;
                        break;
                }
                j++;

                // if row buffer overflows, extend 200 more bytes for the row buffer
                if(flag == false && j >= count) {
                    count = count + extra;
                    buf = copyOf(buf, count);
                }

                // reach the end of a line, convert the row buffer bytes to string
                if(flag == true) {
                    String line = new String(buf,"utf-8");
                    lineNumber ++;

                    // put the new line into reading buffer, ignore the table header
                    if (lineNumber > 1) {
                        buffer.put(line);
                    }
                    flag = false;
                    count = extra;
                    buf = new byte[count];
                    j = 0;
                }

            }

            // the last row
            if(j > 0) {
                String line = new String(buf,"utf-8");
                lineNumber ++;
                buffer.put(line);
            }

            long end = System.currentTimeMillis();
            eof = true;
            memoryMappedFile.close();
            System.out.println("\nreading " + lineNumber + "lines, elapsed:" + (end - start) + "ms");
        } catch (Exception e) {
            System.err.println("reading source file exception");
            e.printStackTrace();
        }
    }

    /**
     * extend the buffer to new length and clone the data into new extended array
     * @param original the original array
     * @param newLength the expected new length of the array
     * @return
     */
    public static byte[] copyOf(byte[] original,int newLength){
        byte[] copy = new byte[newLength];
        System.arraycopy(original,0,copy,0,Math.min(original.length,newLength));
        return copy;
    }

    public BlockingQueue<String> getBuffer() {
        return buffer;
    }

    public boolean isEof() {
        return eof;
    }
}
