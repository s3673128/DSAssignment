import java.time.LocalDateTime;
import java.time.Month;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * source file row data transformer
 */
public class DataResolver {

    // month name and its code
    // JANUARY -> 1
    // ......
    // DECEMBER -> 12
    private static Map<String, Integer> monthMap;
    static {
        monthMap = Arrays.stream(Month.values()).collect(Collectors.toMap(e -> e.name(), e -> e.getValue()));
    }

    /**
     * deserialize a string line to an entity
     * @param line
     * @return
     */
    public static PedestrianData resolve(String line) {
        // ID,Date_Time,Year,Month,Mdate,Day,Time,Sensor_ID,Sensor_Name,Hourly_Counts
        // 2887628,11/01/2019 05:00:00 PM,2019,November,1,Friday,17,34,Flinders St-Spark La,300
        if (line == null || line.equals("")) {
            return null;
        }

        PedestrianData pedestrianData = null;
        try {

            String[] splitArr = line.split(",");

            // calculate the Date_Time
            LocalDateTime time = LocalDateTime.of(Integer.valueOf(splitArr[2]),
                    monthMap.get(splitArr[3].toUpperCase()),
                    Integer.valueOf(splitArr[4]),
                    Integer.valueOf(splitArr[6]),
                    0);

            /* sensor data */
            Sensor sensor = new Sensor();
            sensor.setSensorId(Integer.valueOf(splitArr[7]));
            sensor.setSensorName(splitArr[8]);

            /* pedestrian data */
            pedestrianData = new PedestrianData();
            pedestrianData.setId(Integer.valueOf(splitArr[0]));
            pedestrianData.setDateTime(time);
            pedestrianData.setSensorId(sensor.getSensorId());
            pedestrianData.setSensor(sensor);
            pedestrianData.setHourlyCounts(Integer.valueOf(splitArr[9].trim()));

        } catch (Exception e) {
            System.err.println("invalid row content:" + line);
            e.printStackTrace();
        }
        return pedestrianData;
    }
}
