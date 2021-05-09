package au.edu.rmit.entity;

import java.io.Serializable;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * pedestrian entity
 */
public class PedestrianData implements Serializable {

    private static final long serialVersionUID = 7450522279103175888L;

    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHH");

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
     * Day
     */
    private DayOfWeek dayOfWeek;
    /**
     * Hourly_Counts
     */
    private int hourlyCounts;
    /**
     * au.edu.rmit.entity.Sensor data
     */
    private Sensor sensor;
    /**
     * index field
     */
    private String sdtName;

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

    public String getDateTimeStr() {
        return dtf.format(dateTime);
    }

    public void setDateTime(LocalDateTime dateTime) {
        this.dateTime = dateTime;
    }

    public DayOfWeek getDayOfWeek() {
        return dayOfWeek;
    }

    public void setDayOfWeek(DayOfWeek dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
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

    public void setSdtName(String sdtName) {
        this.sdtName = sdtName;
    }

    /**
     * query text SDT_NAME
     * @return
     */
    public String getSdtName() {
        return sdtName;
    }

    @Override
    public String toString() {
        return "PedestrianData{" +
                "id=" + id +
                ", sensorId=" + sensorId +
                ", dateTime=" + getDateTimeStr() +
                ", dayOfWeek=" + dayOfWeek.name() +
                ", hourlyCounts=" + hourlyCounts +
                ", sensor=" + sensor.getSensorName() +
                '}';
    }
}
