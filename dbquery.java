package au.edu.rmit;

import au.edu.rmit.btree.BTree;
import au.edu.rmit.common.Address;
import au.edu.rmit.common.ByteUtil;
import au.edu.rmit.common.Config;
import au.edu.rmit.common.Property;
import au.edu.rmit.entity.PedestrianData;
import au.edu.rmit.entity.Sensor;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * utility for searching data from heap file
 */
public class dbquery {

    public static List<PedestrianData> query(String filePath, String[] args) {
        if (args.length != 1) {
            System.err.println("please input query condition");
            return Collections.emptyList();
        }
        // query condition
        String[] argsArray = args[0].split("=");
        // condition property
        String property = argsArray[0];
        // condition value
        String propertyValue = argsArray[1];
        Property p = Property.from(property);
        if (p == null) {
            System.err.println("invalid property");
            return Collections.emptyList();
        }

        PageQuery pageQuery = new PageQuery(filePath, p, propertyValue);
        if (index.secondaryIndex.containsKey(p)) {
            /* query with index */

            // fetch the data location in heap file
            BTree<String, List<Integer>> secondaryTree = index.secondaryIndex.get(p);
            List<Integer> idList = secondaryTree.search(propertyValue);
            List<Address> addressList = idList.stream().map(id -> index.pkTree.search(id))
                    .sorted(Comparator.comparing(Address::getPageNo).thenComparing(Address::getRecordNumber))
                    .collect(Collectors.toList());

            // fetch data from specific location
            return pageQuery.execute(addressList);
        } else {
            /* query without index */
            return pageQuery.execute(null);
        }

    }

    /**
     * the query utility to query text in a heap file
     */
    static class PageQuery {

        /**
         * query property
         */
        private Property property;
        /**
         * query text
         */
        private String propertyValue;

        private String filePath;

        /**
         * the page size of target heap file
         */
        private int pageSize;

        /**
         * file pointer
         */
        private RandomAccessFile randomAccessFile;

        /**
         * file channel to read data from file
         */
        private FileChannel fileChannel;

        public PageQuery(String filePath, Property p, String propertyValue) {
            this.filePath = filePath;
            this.pageSize = Integer.valueOf(filePath.substring(filePath.lastIndexOf(".") + 1));
            this.property = p;
            this.propertyValue = propertyValue;
        }

        /**
         * proceed the query
         * @param addressList If addressList is not empty, fetch data from specific page number and record number,
         *                    otherwise, traverse the whole heap file and match the query condition
         */
        public List<PedestrianData> execute(List<Address> addressList) {
            List<PedestrianData> result = Collections.emptyList();
            try {

                // open file channel
                randomAccessFile = new RandomAccessFile(filePath, "r");
                fileChannel = randomAccessFile.getChannel();

                if (addressList == null || addressList.size() == 0) {
                    // traverse query
                    result = pageQuery();
                } else {
                    // index query
                    result = indexQuery(addressList);
                }

            } catch (Exception e) {
                System.err.println("reading heap file failed");
                e.printStackTrace();
            } finally {
                release(randomAccessFile, fileChannel);
            }
            return result;
        }

        // fetch data from specific location of heap file
        private List<PedestrianData> indexQuery(List<Address> addressList) throws IOException {
            List<PedestrianData> result = new ArrayList<>();

            // initialize byte buffer
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);

            int pageCursor = 0;

            int addressCursor = 0;

            // read the heap file
            while (fileChannel.read(byteBuffer) >= 0) {

                for (int i = addressCursor; i < addressList.size(); i++) {
                    Address address = addressList.get(i);
                    int pageNo = address.getPageNo();
                    addressCursor = i;
                    if (pageNo > pageCursor) {
                        break;
                    }

                    // record data byte array
                    byte[] recordArray = ByteUtil.subArray(byteBuffer.array(), address.getRecordNumber() * Config.HEAP_RECORD_LENGTH + 4, Config.HEAP_RECORD_LENGTH);

                    // deserialize record bytes
                    PedestrianData pedestrianData = deserialize(recordArray);
                    result.add(pedestrianData);
                }

                // clear buffer and prepare for next page
                byteBuffer.clear();
                pageCursor ++;

                if (addressList.size() == result.size()) {
                    break;
                }
            }

            return result;
        }

        // traverse query
        private List<PedestrianData> pageQuery() throws IOException {
            List<PedestrianData> result = new ArrayList<>();

            // initialize byte buffer
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);

            // read the heap file
            while (fileChannel.read(byteBuffer) >= 0) {
                // query in page
                List<PedestrianData> pageData = queryInPage(byteBuffer.array());
                result.addAll(pageData);
                // clear buffer and prepare for next page
                byteBuffer.clear();
            }

            return result;
        }

        /**
         * query specific text in one heap page
         * @param array page data byte array
         */
        private List<PedestrianData> queryInPage(byte[] array) {
            // fetch record count in page from page header
            int recordCount = ByteUtil.getInt(array, 0);

            List<PedestrianData> result = new ArrayList<>();

            /* traverse data in page, handle each record split from page data */
            for (int i = 0; i < recordCount; i++) {
                // record data byte array
                byte[] recordArray = ByteUtil.subArray(array, i * Config.HEAP_RECORD_LENGTH + 4, Config.HEAP_RECORD_LENGTH);

                // compare record bytes and the query text
                PedestrianData pedestrianData = deserialize(recordArray);
                if ((property == Property.ID && Integer.valueOf(propertyValue).equals(pedestrianData.getId()))
                        || (property == Property.Date_Time && propertyValue.equals(pedestrianData.getDateTimeStr()))
                        || (property == Property.Year && Integer.valueOf(propertyValue).equals(pedestrianData.getDateTime().getYear()))
                        || (property == Property.Month && propertyValue.toUpperCase().equals(pedestrianData.getDateTime().getMonth().name()))
                        || (property == Property.Mdate && Integer.valueOf(propertyValue).equals(pedestrianData.getDateTime().getDayOfMonth()))
                        || (property == Property.Day && propertyValue.toUpperCase().equals(pedestrianData.getDateTime().getDayOfWeek().name()))
                        || (property == Property.Time && Integer.valueOf(propertyValue).equals(pedestrianData.getDateTime().getHour()))
                        || (property == Property.Hourly_Counts && Integer.valueOf(propertyValue).equals(pedestrianData.getHourlyCounts()))
                        || (property == Property.Sensor_Id && Integer.valueOf(propertyValue).equals(pedestrianData.getSensorId()))
                        || (property == Property.Sensor_Name && propertyValue.equals(pedestrianData.getSensor().getSensorName()))
                        || (property == Property.SDT_NAME && propertyValue.equals(pedestrianData.getSdtName()))
                ) {
                    result.add(pedestrianData);
                }
            }
            return result;
        }

        /**
         * deserialize record bytes array, return an transferred entity
         * @param array record bytes array
         */
        private PedestrianData deserialize(byte[] array) {
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
        private void release(RandomAccessFile outputFile, FileChannel fileChannel) {
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
}
