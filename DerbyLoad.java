import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * Derby client for loading data into Derby database
 */
public class DerbyLoad {

    public static void main(String[] args) {

        // start a thread to read the source file
        CsvReader csvReader = new CsvReader(Config.SOURCE_FILE_PATH);
        Thread readThread = new Thread(csvReader);
        readThread.start();

        // start a thread to write data into derby database
        Writer writer = new Writer(csvReader);
        Thread writeThread = new Thread(writer);
        writeThread.start();

        try {
            /* exit the import util after reading and writing threads finish */
            writeThread.join();
            readThread.join();
            System.out.println("Derby import done.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Thread writing data into derby tables
     */
    static class Writer implements Runnable {

        private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        /**
         * reading thread
         */
        private CsvReader csvReader;

        /**
         * reading data buffer
         */
        private BlockingQueue<String> buffer;

        /**
         * Derby database connection
         */
        private Connection connection;

        /**
         * Statement instance to execute sql statements
         */
        private Statement statement;

        public Writer(CsvReader csvReader) {
            this.csvReader = csvReader;
            this.buffer = csvReader.getBuffer();
        }

        @Override
        public void run() {

            int lineNumber = 0;

            try{
                long start = System.currentTimeMillis();

                /* get connection */
                connection = JdbcTemplate.getConnection();
                JdbcTemplate.beginTx(connection);
                statement = connection.createStatement();

                // the sensor ids already saved, in case not to save duplicated sensor
                Set<Integer> sensorSet = new HashSet<>();

                // batch data need to be saved
                List<PedestrianData> pedestrianDataList = new ArrayList<>();

                // deal with the buffer until the reading thread reaches the end of source file
                while(!csvReader.isEof() || !buffer.isEmpty()) {
                    // get one line from the buffer
                    String line = buffer.take();
                    System.out.println("---"+csvReader.isEof()+"---"+line+"---"+buffer.isEmpty());
                    // deserialize
                    PedestrianData pedestrianData = DataResolver.resolve(line);

                    /* add to batch, if the batch size comes to 100, save and clear it */
                    pedestrianDataList.add(pedestrianData);
                    if (pedestrianDataList.size() == 100) {
                        batchSave(pedestrianDataList, sensorSet);
                        pedestrianDataList.clear();
                    }

                    lineNumber ++;
                    if (lineNumber % 10000 == 0) {
                        System.out.print("+");
                    }
                }

                // save the rest data in the batch
                if (pedestrianDataList.size() > 0) {
                    batchSave(pedestrianDataList, sensorSet);
                }

                long end = System.currentTimeMillis();
                System.out.println("\nsaved " + lineNumber + "lines, elapsed:" + (end - start) + "ms");
            } catch(Exception e) {
                System.err.println("writing data into Derby interrupted");
                JdbcTemplate.rollback(connection);
                e.printStackTrace();
            } finally {
                JdbcTemplate.releaseConnection(statement, connection);
            }
        }

        /**
         * save the data into database
         * @param pedestrianDataList the data to be saved
         * @param sensorSet sensor ids those are already saved
         * @return
         * @throws SQLException
         */
        private int batchSave(List<PedestrianData> pedestrianDataList, Set<Integer> sensorSet) throws SQLException {
            if (pedestrianDataList == null || pedestrianDataList.size() == 0) {
                return 0;
            }

            List<Sensor> sensorList = new ArrayList<>();

            /* concatenate the sql and insert pedestrian data into table */
            StringBuffer pedestrianSql = new StringBuffer("insert into pedestrian_data(id,sensor_id,date_time,hourly_counts) values ");
            for (int i = 0; i < pedestrianDataList.size(); i++) {
                PedestrianData pedestrianData = pedestrianDataList.get(i);
                pedestrianSql.append("(").append(pedestrianData.getId()).append(",")
                        .append(pedestrianData.getSensorId()).append(",'")
                        .append(dtf.format(pedestrianData.getDateTime())).append("',")
                        .append(pedestrianData.getHourlyCounts()).append(")");
                if (i < pedestrianDataList.size() - 1) {
                    pedestrianSql.append(",");
                }
                if (!sensorSet.contains(pedestrianData.getSensorId())) {
                    sensorList.add(pedestrianData.getSensor());
                    sensorSet.add(pedestrianData.getSensorId());
                }
            }
            statement.execute(pedestrianSql.toString());

            /* concatenate the sql and insert sensor data into table */
            if (sensorList.size() > 0) {
                StringBuffer sensorSql = new StringBuffer("insert into sensor(sensor_id,sensor_name) values ");
                for (int i = 0; i < sensorList.size(); i++) {
                    Sensor sensor = sensorList.get(i);
                    sensorSql.append("(").append(sensor.getSensorId()).append(",'")
                            .append(sensor.getSensorName()).append("')");
                    if (i < sensorList.size() - 1) {
                        sensorSql.append(",");
                    }
                }
                statement.execute(sensorSql.toString());
            }

            // commit the sql
            JdbcTemplate.commit(connection);
            return pedestrianDataList.size();
        }
    }
}
