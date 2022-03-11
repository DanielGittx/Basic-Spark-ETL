import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Utilities {

    public  Utilities (){};

    SparkSession spark;

    public Utilities(SparkSession spark) {
        this.spark = spark;
    }

    public  String ReadFile(String fragment) throws IOException {
        Path path = Paths.get(fragment);
        File file = new File(path.toString());
        BufferedReader reader = new BufferedReader(new FileReader(file));

        String content = "";
        String line = "";

        while ((line = reader.readLine()) != null)
            content = content + line + "\n";

        reader.close();

        return content;
    }

    public  String fileReaderHDFS(String hadoopStringPath) throws IOException {
        //Set Hadoop Configs
        Configuration hdfs_configuration = setHadoopConfiguration();

        //Initialize hdfs
        FileSystem hadoopFileSystem = getHDFS(hdfs_configuration);
        org.apache.hadoop.fs.Path hadoop_path = getHDFSPath(hadoopStringPath);
        //FIXME or a NOTE:- Though faster/more efficient the StringBuilder library isn't thread safe!
        StringBuilder sb = new StringBuilder();
        String line;
        //Read file from hdfs into the String builder
        BufferedReader br = new BufferedReader(new InputStreamReader(hadoopFileSystem.open(hadoop_path)));

        while ((line = br.readLine()) != null) {
            sb.append(line + "\n");
        }

        //Close connection. I/O Management
        br.close();
        hadoopFileSystem.close();
        return sb.toString();
    }

    public  boolean checkIfPathExists(String hadoopStringPath) throws IOException {
        //Set Hadoop Configs
        Configuration hdfs_configuration = setHadoopConfiguration();
        //Initialize hadoop file system object
        FileSystem hadoopFileSystem = getHDFS(hdfs_configuration);
        // get hadoop path
        org.apache.hadoop.fs.Path hadoop_path = getHDFSPath(hadoopStringPath);
        // Evaluate if path exists
        return hadoopFileSystem.exists(hadoop_path) ? true : false;
    }

    public  org.apache.hadoop.fs.Path getHDFSPath(String hadoopStringPath) throws IOException {
        org.apache.hadoop.fs.Path hadoop_path = new org.apache.hadoop.fs.Path(hadoopStringPath);
        return hadoop_path;
    }

    public  Configuration setHadoopConfiguration() {
        Configuration confFS = new Configuration();
        confFS.addResource("/data02/nifi/hiveconfigs/core-site.xml");
        confFS.addResource("/data02/nifi/hiveconfigs/hdfs-site.xml");
        confFS.addResource("/data02/nifi/hiveconfigs/hive-site.xml");
        return confFS;

    }

    public  FileSystem getHDFS(Configuration hdfs_configuration)
            throws IOException {
        FileSystem hadoopFileSystem = FileSystem.newInstance(hdfs_configuration);
        return hadoopFileSystem;
    }

    public  Date stringToDate(String dateValue) throws ParseException {
        SimpleDateFormat originalFormat = new SimpleDateFormat("yyyyMMdd");
        Date date = originalFormat.parse(dateValue);
        SimpleDateFormat newFormat = new SimpleDateFormat("yyyy-MM-dd");

        String formatedDate = newFormat.format(date);
        //System.out.println("ROW OUTPUT : " +formatedDate);

        return date;
    }

    public  LocalDateTime convertToLocalDateTimeViaMilisecond(Date dateToConvert) {
        return Instant.ofEpochMilli(dateToConvert.getTime())
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
    }

    public  String FormatDateToString(Date date, String format) {
        DateFormat dateFormat = new SimpleDateFormat(format);

        String dateString = dateFormat.format(date);

        return dateString;
    }


    public  BigInteger fib(BigInteger n)              //Big sum for application stress testing
    {
        if (n.compareTo(BigInteger.ONE) == -1 || n.compareTo(BigInteger.ONE) == 0) return n;
        else return fib(n.subtract(BigInteger.ONE)).add(fib(n.subtract(BigInteger.ONE).subtract(BigInteger.ONE)));
    }

    public  List getListOfFormattedPaths(String basePath) {
        List listOfPaths = new ArrayList<>();

        for (int x = 10; x < 42; x++) {
            listOfPaths.add(String.format(basePath, x));
            //System.out.println("Path: -  " +String.format(basePath , x));
        }
        //System.out.println("ListOfPaths" +listOfPaths.get(0)+ listOfPaths.get(10)) ;
        return listOfPaths;
    }


    public  SparkConf getSparkConfigurations() {
        //return new SparkConf();
        /* Local testing  */
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Spark SQL Aggregates ");
        return conf;
    }


    public String appendFileSystem(String path) {
        String fsAppender = "file:";
        return fsAppender + path;
    }

    public  Dataset<Row> parquetSparkRead(SparkSession sparkSession, String fileReadPath, String outputFilePath) {

        Dataset<Row> DF = sparkSession.read()
                .option("mode", "DROPMALFORMED")
                .option("inferSchema", true)
                .option("header", true)

                //.schema(schema)
                .parquet(fileReadPath);

        return DF;

    }


    public  Dataset<Row> csvSparkRead(SparkSession sparkSession, String fileReadPath) {

        System.out.println("CSV Read: START");
        Dataset<Row> dfCsvData = sparkSession.read()
                .option("mode", "DROPMALFORMED")
                .option("inferSchema", true)
                .option("header", true)
                //.schema(schema)
                .csv(fileReadPath);

        System.out.println("CSV Read: EXIT");

        return dfCsvData;

    }

    // Union
    public  Dataset<Row> CombineDataFrames(List<Dataset<Row>> dataframes) {
        Dataset<Row> final_df = null;

        int counter = 0;
        for (Dataset<Row> df : dataframes) {
            if (counter == 0)
                final_df = df;
            else
                final_df = final_df.union(df);

            counter++;
        }
        return final_df;
    }



}
