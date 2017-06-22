import Util.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public class Test {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("InternetBarFilter").getOrCreate();
        SQLContext sqlContext = spark.sqlContext();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");
        JavaRDD<String> inputData = spark.read().textFile("file:///C:/Users/Administrator/Desktop/receipt.txt").toJavaRDD();


        long startTime = System.currentTimeMillis();
        JavaRDD<InfoUnit> parsedData = inputData.filter(s -> !Objects.equals(s.split(";")[6], "")).map(s -> {
            String[] str = s.split(";");
            InfoUnit iu = new InfoUnit();
            iu.setIDCard(str[6]);
            iu.setMarketID(str[3]);
            iu.setTime(str[0]);
            return iu;
        });

        String id = "102051";             //ID
        String market = "610";
        long timeFilter = 5;              //判定时间
        int itemFrequency = 2;            //判定频次
        List<String> lid = new ArrayList<String>(){{add(id);add(market);}};


        Dataset<Row> data = sqlContext.createDataFrame(parsedData, InfoUnit.class).cache();
        data.createOrReplaceTempView("sourceData");

        StringJoiner sqlJoiner = new StringJoiner("='", "SELECT IDCard,marketID,time FROM sourceData WHERE ", "'");
        sqlJoiner.add("IDCard").add(id);

        Dataset<Row> tmpData1 = sqlContext.sql("SELECT * FROM sourceData");

        List<String> records = sqlContext.sql(sqlJoiner.toString()).toJavaRDD().map(r -> {
            StringJoiner str = new StringJoiner(",");
            str.add(r.getString(0)).add(r.getString(1)).add(r.getString(2));
            return str.toString();
        }).distinct().collect();

        //List<List<String>> itemCollection = new ArrayList<>();
        //System.out.println("The initial item:"+items.first());
        List<Set<List<String>>> itemCollection = new ArrayList<>();

        records.forEach(record -> {
            String[] str = record.split(",");

            String marketID = str[1];
            String timeStamp = str[2];
            Dataset<Row> tmpData2 = tmpData1.filter((FilterFunction<Row>) row -> marketID.equals(row.getString(1))).filter((FilterFunction<Row>) row -> compare_date(row.getString(2), timeStamp));
            //System.out.println("The count of tmpData2:"+tmpData2.count());

            JavaRDD<List<String>> tmpData3 = tmpData2.toJavaRDD().map((Function<Row, List<String>>) row -> {
                List<String> ls = new ArrayList<>();
                ls.add(row.getString(0));
                ls.add(row.getString(1));
                return ls;
            });


            List<List<String>> itemSet = tmpData3.collect();
            Set<List<String>> item = new HashSet<>();
            item.addAll(itemSet);
            itemCollection.add(item);
        });

        JavaRDD<Set<List<String>>> inputItems = jsc.parallelize(itemCollection, 3).cache();

        Set<List<String>> filterResult = FrequencyItems.getFrequencyItems(inputItems,itemFrequency,2,lid);
        filterResult.forEach(System.out::println);

        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");    //输出程序运行时间

        spark.close();
    }


    private static boolean compare_date(String DATE1, String DATE2) {

//        long timeDiff;
//        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//        try {
//            Date dt1 = df.parse(DATE1);
//            Date dt2 = df.parse(DATE2);
//            timeDiff = Math.abs((dt2.getTime() - dt1.getTime())/(60*1000));
//            //System.out.println("dt2-dt1="+timeDiff);
//
//            return timeDiff < 5;
//        } catch (Exception exception) {
//            exception.printStackTrace();
//        }
//        return false;

        long timeDiff;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date dt1 = df.parse(DATE1);
            Date dt2 = df.parse(DATE2);
            timeDiff = Math.abs((dt2.getTime() - dt1.getTime()) / (24 * 60 * 60 * 1000));
            //System.out.println("dt2-dt1="+timeDiff);

            return timeDiff < 1;
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return false;
    }


}

