import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * derby jdbc template
 */
public class JdbcTemplate {

    /**
     * get database connection
     */
    public static Connection getConnection() throws Exception {
        try {
            Connection connection = DriverManager.getConnection(Config.DB_URL);
            System.out.println("database connected");
            return connection;
        } catch (SQLException e) {
            System.err.println("Cannot get connection, please make sure your Derby server is ready");
            e.printStackTrace();
        }
        return null;
    }

    /**
     * commit transaction
     */
    public static void commit(Connection connection) {
        try {
            connection.commit();
        }
        catch (SQLException e) {
            System.err.println("database commit failed");
            e.printStackTrace();
        }
    }

    /**
     * start a database transaction
     */
    public static void beginTx(Connection connection) {
        try {
            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            System.err.println("database transaction start failed");
            e.printStackTrace();
        }
    }

    /**
     * rollback the transaction
     */
    public static void rollback(Connection connection) {
        try {
            connection.rollback();
        }
        catch (SQLException e) {
            System.err.println("database transaction rollback failed");
            e.printStackTrace();
        }
    }

    /**
     * release database connection
     */
    public static void releaseConnection(Statement statement, Connection connection) {
        try {
            statement.close();
            connection.close();
        }
        catch (SQLException e) {
            System.err.println("failed to release database connection");
            e.printStackTrace();
        }
    }
}