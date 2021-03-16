import java.time.LocalDateTime;

/**
 * pedestrian entity
 */
public class PedestrianData {

    /**
     * ID
     */
    private int id;
    /**
     * Sensor_ID
     */
    private int sensorId;
    /**
     * Date_Time
     */
    private LocalDateTime dateTime;
    /**
     * Hourly_Counts
     */
    private int hourlyCounts;
    /**
     * Sensor data
     */
    private Sensor sensor;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getSensorId() {
        return sensorId;
    }

    public void setSensorId(int sensorId) {
        this.sensorId = sensorId;
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public void setDateTime(LocalDateTime dateTime) {
        this.dateTime = dateTime;
    }

    public int getHourlyCounts() {
        return hourlyCounts;
    }

    public void setHourlyCounts(int hourlyCounts) {
        this.hourlyCounts = hourlyCounts;
    }

    public Sensor getSensor() {
        return sensor;
    }

    public void setSensor(Sensor sensor) {
        this.sensor = sensor;
    }
}
