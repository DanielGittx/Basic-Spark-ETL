import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class DatabaseConfigs {

    public static String databaseDriver(String dbTech) {

        if (dbTech.equals("oracle")) {
            return "oracle.jdbc.driver.OracleDriver";
        } else if (dbTech.equals("postgress")) {
            return "org.postgresql.Driver";
        } else if (dbTech.equals("hive")) {
            return "org.apache.hadoop.hive.jdbc.HiveDriver";
        } else if (dbTech.equals("mssql")) {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        } else if (dbTech.equals("mysql")) {
            return "com.mysql.jdbc.Driver";
        }

        return "Unknown database driver : contact Big Data Engineering!";      //we should never get here! :(
    }


    public static Properties setDatabaseProperties(String userName, String password, String dbTech) {
        System.out.println(" Setting database connection properties...\n");
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", userName);
        connectionProperties.put("password", password);
        connectionProperties.put("driver", databaseDriver(dbTech));
        System.out.println(" Database connection properties set...\n");
        return connectionProperties;
    }

    public static Connection getDataseConnection(String url, String userName, String password, String dbTech) {

        String driver = databaseDriver(dbTech);

        Connection con;
        try {
            Class.forName(driver).newInstance();
            con = DriverManager.getConnection(
                    url,
                    userName,
                    password
            );
            System.out.println("Connected to database!");
            return con;

        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("\n\tDatabase Connection failure...!\n " + e.getMessage() + "\n");
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void writeDatasetRowsIntoDatabase(Dataset<Row> df, String dbTech) {
        // Note : This is slow!
        df.foreach((Row row) -> {

            System.out.println("ROW OUTPUT : " + row.get(1));          // 0 is 1st Row etc
            Connection postgressConnection = getDataseConnection("", "", "", dbTech);
            Statement statement = postgressConnection.createStatement();
            // create the mysql insert preparedstatement
            PreparedStatement preparedStmt = postgressConnection.prepareStatement("");
            preparedStmt.setString(1, row.get(0).toString());
            preparedStmt.setString(2, row.get(1).toString());
            // execute the preparedstatement
            preparedStmt.execute();
            postgressConnection.close();


        });
    }

    public static void invokeStoredProcedure(String stoProc, String dbTech) {
        LocalDate stoProcDate = LocalDate.now().minusDays(0);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedString = stoProcDate.format(formatter);
        String formattedStoredProcedure = String.format(stoProc, formattedString);
        System.out.println("\nSTOPROC TO BE EXECUTED \n" + formattedStoredProcedure);
        try {
            Connection connection = getDataseConnection(
                    "", "", "", dbTech);


            CallableStatement callableStatement = connection.prepareCall(formattedStoredProcedure);
            callableStatement.execute();
            callableStatement.close();
            connection.close();
        } catch (SQLException sqEx) {
            System.out.println(" SQL Exception : " + sqEx.getMessage());
        }
        System.out.println(" STORED PROCEDURE DONE ");
    }

}
