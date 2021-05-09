package au.edu.rmit;

import au.edu.rmit.btree.BTreeInnerNode;
import au.edu.rmit.btree.BTreeLeafNode;
import au.edu.rmit.btree.BTreeNode;
import au.edu.rmit.btree.TreeNodeType;
import au.edu.rmit.common.Address;
import au.edu.rmit.btree.BTree;
import au.edu.rmit.common.ByteUtil;
import au.edu.rmit.common.Config;
import au.edu.rmit.common.Property;
import au.edu.rmit.entity.PedestrianData;
import au.edu.rmit.entity.Sensor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * utility for manage index of heap file
 */
public class index {

    private enum Command {
        ADD,
        DROP,
    }

    // primary key index b+ tree
    static BTree<Integer, Address> pkTree = new BTree<>();

    // secondary index
    static Map<Property, BTree<String, List<Integer>>> secondaryIndex = new HashMap<>();

    public static void main(String filePath, String[] args) {
        if (args.length != 2) {
            System.err.println("illegal arguments");
            return;
        }
        String command = args[0];
        String property = args[1];

        // check index property
        Property p = Property.from(property);
        if (p == null) {
            System.err.println("only index on below properties:Date_Time,Year,Month,Mdate,Day,Time,Hourly_Counts,Sensor_Id,Sensor_Name,SDT_NAME");
            return;
        }

        if (command.equalsIgnoreCase(Command.ADD.name())) {
            addIndex(filePath, p);
        } else if (command.equalsIgnoreCase(Command.DROP.name())) {
            dropIndex(p);
        } else {
            System.err.println("unsupported operator");
            System.err.println("please input add or drop as first argument");
            return;
        }
    }

    // drop index
    private static void dropIndex(Property property) {
        if (property == Property.ID) {
            pkTree = new BTree<>();
        } else {
            secondaryIndex.remove(property);
        }
    }

    /**
     * add index for specific property of heap file
     * @param filePath heap file name. e.g. heap.1024
     * @param property see au.edu.rmit.common.Property
     */
    private static void addIndex(String filePath, Property property) {

        // primary key index reset
        pkTree = new BTree<>();
        // secondary index reset
        if (property != Property.ID) {
            secondaryIndex.put(property, new BTree<>());
        }

        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {

            long start = System.currentTimeMillis();

            // open file channel
            randomAccessFile = new RandomAccessFile(filePath, "r");
            fileChannel = randomAccessFile.getChannel();

            // initialize byte buffer
            int pageSize = Integer.valueOf(filePath.replace("heap.", ""));
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);

            int pageNo = 0;
            // read the heap file
            while (fileChannel.read(byteBuffer) >= 0) {
                // index page data
                pageIndex(pageNo, byteBuffer.array(), property);
                // clear buffer and prepare for next page
                byteBuffer.clear();

                pageNo ++;
            }

            long end = System.currentTimeMillis();
            System.out.println("index created, elapsed " + (end - start) + "ms.");

        } catch (Exception e) {
            System.err.println("reading heap file failed");
            e.printStackTrace();
        } finally {
            release(randomAccessFile, fileChannel);
        }
    }

    /**
     * index for page data
     * @param pageNo page number
     * @param pageData page byte data
     * @param property index property
     */
    private static void pageIndex(int pageNo, byte[] pageData, Property property) {
        // fetch record count in page from page header
        int recordCount = ByteUtil.getInt(pageData, 0);

        /* traverse data in page, handle each record split from page data */
        for (int i = 0; i < recordCount; i++) {
            // record data byte array
            byte[] recordArray = ByteUtil.subArray(pageData, i * Config.HEAP_RECORD_LENGTH + 4, Config.HEAP_RECORD_LENGTH);

            // convert record bytes
            PedestrianData record = convert(recordArray);

            // primary key
            pkTree.insert(record.getId(), new Address(pageNo, i));

            // secondary index
            if (property != Property.ID) {
                BTree<String, List<Integer>> secondaryTree = secondaryIndex.get(property);
                if (secondaryTree == null) {
                    secondaryTree = new BTree<>();
                    secondaryIndex.put(property, secondaryTree);
                }
                String key = getPropertyValue(property, record);
                List<Integer> idList = secondaryTree.search(key);
                if (idList == null) {
                    idList = new LinkedList<>();
                    secondaryTree.insert(key, idList);
                }
                idList.add(record.getId());
            }
        }
    }

    // get the property value of record data
    private static String getPropertyValue(Property property, PedestrianData record) {
        String key;
        switch (property) {
            case ID:
                key = String.valueOf(record.getId());
                break;
            case Date_Time:
                key = record.getDateTimeStr();
                break;
            case Year:
                key = String.valueOf(record.getDateTime().getYear());
                break;
            case Month:
                key = record.getDateTime().getMonth().name();
                break;
            case Mdate:
                key = String.valueOf(record.getDateTime().getDayOfMonth());
                break;
            case Day:
                key = record.getDayOfWeek().name();
                break;
            case Time:
                key = String.valueOf(record.getDateTime().getHour());
                break;
            case Sensor_Id:
                key = String.valueOf(record.getSensorId());
                break;
            case Hourly_Counts:
                key = String.valueOf(record.getHourlyCounts());
                break;
            case Sensor_Name:
                key = record.getSensor().getSensorName();
                break;
            case SDT_NAME:
                key = record.getSdtName();
                break;
            default:
                throw new RuntimeException("invalid property:" + property);
        }
        return key;
    }

    /**
     * convert byte data into an entity
     * @param array record bytes array
     */
    private static PedestrianData convert(byte[] array) {
        byte sensorNameOffset = ByteUtil.getByte(array, 0);
        byte sdtNameOffset = ByteUtil.getByte(array, 1);
        int id = ByteUtil.getInt(array, 2);
        long timestamp = ByteUtil.getLong(array, 6);
        byte dayOfWeekIndex = ByteUtil.getByte(array, 20);
        int hourlyCounts = ByteUtil.getInt(array, 22);
        int sensorId = ByteUtil.getInt(array, 26);
        String sensorName = ByteUtil.getString(array, 30, sensorNameOffset - 30).trim();
        String sdtName = ByteUtil.getString(array, sensorNameOffset, sdtNameOffset - sensorNameOffset).trim();

        /* convert bytes to entity */
        Sensor sensor = new Sensor();
        sensor.setSensorId(sensorId);
        sensor.setSensorName(sensorName);
        PedestrianData pedestrianData = new PedestrianData();
        pedestrianData.setId(id);
        pedestrianData.setDateTime(LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), TimeZone.getDefault().toZoneId()));
        pedestrianData.setDayOfWeek(DayOfWeek.of(dayOfWeekIndex));
        pedestrianData.setHourlyCounts(hourlyCounts);
        pedestrianData.setSensorId(sensorId);
        pedestrianData.setSensor(sensor);
        pedestrianData.setSdtName(sdtName);
        return pedestrianData;
    }

    /**
     * release file channel
     * @param outputFile
     * @param fileChannel
     */
    private static void release(RandomAccessFile outputFile, FileChannel fileChannel) {
        try {
            if (fileChannel != null) {
                fileChannel.close();
            }
            if (outputFile != null) {
                outputFile.close();
            }
        } catch (Exception e) {
            System.err.println("close heap file failed");
        }
    }
}
