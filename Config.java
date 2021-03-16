import java.util.ResourceBundle;

/**
 * global config
 */
public class Config {

    /**
     * Derby connection url
     */
    public static String DB_URL;

    /**
     * file source path
     */
    public static String SOURCE_FILE_PATH;

    static {
        ResourceBundle bundle = ResourceBundle.getBundle("config");
        DB_URL = bundle.getString("jdbc.url");
        SOURCE_FILE_PATH = bundle.getString("file.source.path");
    }
}
