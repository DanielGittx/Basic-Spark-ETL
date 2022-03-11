
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.SparkConf;

public class SparkJobs implements IConstants {

    public void runSPARKTasks(SparkSession sparkSession, String[] commandLineArgs) throws Exception {


        System.out.println("\n------------------ START RUN: runSPARKTasks---------------------\n");

        Dataset<Row> dfToCassandra = runSparkSQLQuery(
                sparkSession,
                "databaseUserName",
                "databasePassword",
                "databaseUrl",
                "dbDriver",
                QUERY1);

        runCassandra(sparkSession, dfToCassandra);


        System.out.println("\n------------------ END RUN: runSPARKTasks---------------------\n");

    }

    private static Dataset<Row> runSparkSQLQuery(
            SparkSession sparkSession,
            String databaseUserName,
            String databasePassword,
            String databaseUrl,
            String dbDriver,
            String sqlQuery
    ) {

        //SparkSession sparkSession = getSparkSession();
        long start = System.currentTimeMillis();
        System.out.println(" Spark SQL session ready ...\nNow executing query");
        Dataset<Row> df = null;

        df = sparkSession.read()
                .format("jdbc")
                .option("url", databaseUrl)
                .option("driver", "databaseDriver")
                .option("user", databaseUserName)
                .option("password", databasePassword)

//              .option("ssl", "true")
//              .option("sslrootcert", "/home/path/to/certs/root.crt")
//              .option("sslcert", "/home/path/to/certs/postgres.crt")
//              .option("sslkey", "/home/path/to/certs/postgresql.key")

                .option("query", sqlQuery)               //For Apache Spark >= 2.4.4
                //.option("dbtable", sqlQuery)           //For Apache Spark <= 2.4.3
                .load();


        long end = System.currentTimeMillis(); //test
        float sec = (end - start) / 1000F;
        System.out.println("Job execution period:   : " + sec + " seconds" + "\nSql query \n" + sqlQuery + "\ncompleted!\nNow exiting...");

        return df;

    }


    public void runCassandra(SparkSession spark, Dataset<Row> df) {
        System.out.println("\n------------------ START Cassandra ---------------------\n");

        // write to cassandra
        df.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "keyspace_name")
                .option("table", "table_name")
                .option("spark.cassandra.connection.host", "127.0.0.1")
                .option("spark.cassandra.connection.port", "9042")
                //.option("confirm.truncate", "true") // This is to confirm mode is overwrite. Its a required field
                .option("header", "false")
                .option("spark.cassandra.output.ignoreNulls", true)
                //.mode("overwrite")
                .mode(SaveMode.Append)
                .save();

        System.out.println("\n------------------ END Cassandra ---------------------\n");
    }



}
