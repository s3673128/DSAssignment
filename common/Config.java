package au.edu.rmit.common;

import java.util.ResourceBundle;

/**
 * global config
 */
public class Config {

    /**
     * heap file record bytes
     */
    public static int HEAP_RECORD_LENGTH;

    static {
        ResourceBundle bundle = ResourceBundle.getBundle("au.edu.rmit.common.config");
        HEAP_RECORD_LENGTH = Integer.parseInt(bundle.getString("heap.record.length"));
    }
}
