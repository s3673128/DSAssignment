package au.edu.rmit.entity;

import java.io.Serializable;

/**
 * sensor entity
 */
public class Sensor implements Serializable {

    private static final long serialVersionUID = -7978678661396297401L;

    /**
     * Sensor_ID
     */
    private int sensorId;
    /**
     * Sensor_Name
     */
    private String sensorName;

    public int getSensorId() {
        return sensorId;
    }

    public void setSensorId(int sensorId) {
        this.sensorId = sensorId;
    }

    public String getSensorName() {
        return sensorName;
    }

    public void setSensorName(String sensorName) {
        this.sensorName = sensorName;
    }
}
