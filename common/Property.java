package au.edu.rmit.common;

/**
 * data property
 */

public enum Property {
    ID,
    Date_Time,
    Year,
    Month,
    Mdate,
    Day,
    Time,
    Hourly_Counts,
    Sensor_Id,
    Sensor_Name,
    SDT_NAME;

    /**
     * deserialize from enum name
     * @param str
     * @return
     */
    public static Property from(String str) {
        if (str == null || str.equals("")) {
            return null;
        }
        for (Property value : values()) {
            if (value.name().equalsIgnoreCase(str)) {
                return value;
            }
        }
        return null;
    }
}
