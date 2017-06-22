import Util.InfoUnit;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.*;

import scala.Tuple2;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
public class AllTest {

    static String marketID;
    static String timeStamp;
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("InternetBarFilter").getOrCreate();
        SQLContext sqlContext = spark.sqlContext();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");
        JavaRDD<String> inputData = spark.read().textFile("file:///C:/Users/Administrator/Desktop/receipt.txt").toJavaRDD();

        JavaRDD<InfoUnit> parsedData = inputData.filter(s -> !Objects.equals(s.split(";")[6], "")).map(s -> {
            String[] str = s.split(";");
            InfoUnit iu = new InfoUnit();
            iu.setIDCard(str[6]);
            iu.setMarketID(str[3]);
            iu.setTime(str[0]);
            return iu;
        });
//
//        String id = "102051";             //ID
//        long timeFilter = 5;              //判定时间
//        double itemFrequency = 2.0;            //判定频次

        Dataset<Row> data = sqlContext.createDataFrame(parsedData,InfoUnit.class).cache();
        data.createOrReplaceTempView("sourceData");
        JavaPairRDD<String,List<String>> dataTmp1 = data.toJavaRDD().mapToPair(row -> {
            String id = row.getString(0);
            String record = id+","+row.getString(1)+","+row.getString(2);
            return new Tuple2<>(id,record);
        }).distinct().reduceByKey((s1, s2) -> s1+"-"+s2 ).mapValues(s -> Arrays.asList(s.split("-")));


        JavaPairRDD<String,List<String>> dataTmp2 = dataTmp1.mapValues(records -> {

            String id = records.get(0).split(",")[0];
            List<List<String>> itemCollection = new ArrayList<>();
            records.forEach(record -> {
                String[] str = record.split(",");
                marketID = str[1];
                timeStamp = str[2];
//            InfoUnit iu = new InfoUnit();
//            iu.setMarketID(marketID);
//            iu.setTime(timeStamp);
                Dataset<Row> tmpData2 = data.filter((FilterFunction<Row>) row -> row.getString(1).equals(marketID)).filter((FilterFunction<Row>) row -> compare_date(row.getString(2), timeStamp));
                //System.out.println("The count of tmpData2:"+tmpData2.count());

                JavaRDD<String> tmpData3 = tmpData2.toJavaRDD().map(r -> r.getString(0)).mapToPair(ss -> new Tuple2<>(1, ss)).reduceByKey((s1, s2) -> s1 + "," + s2).values();
                String[] itemSet = tmpData3.collect().get(0).split(",");
                List<String> item = new ArrayList<>();
                for (String anItemSet : itemSet) {
                    //如果集合里面没有相同的元素才往里存
                    if (!item.contains(anItemSet)) {
                        item.add(anItemSet);
                    }
                }
                itemCollection.add(item);
            });
            JavaRDD<List<String>> items = jsc.parallelize(itemCollection, 3);

            long recordNumber = items.count();
            //System.out.println("Total number of bills: "+recordNumber);

            FPGrowth fpg = new FPGrowth()
                    .setMinSupport(2.0 / recordNumber)                              //最小支持度
                    .setNumPartitions(10);
            FPGrowthModel<String> model = fpg.run(items);


            List<String> filterResult = new ArrayList<>();
            for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {

                boolean flag = true;
                String str1 = itemset.javaItems().toString().replace("[", "").replace("]", "").replace(" ", "");
                long freq1 = itemset.freq();
                if (!str1.contains(id) || !str1.contains(",")) continue;
                //System.out.println("Str1:"+str1+","+freq1);

                for (FPGrowth.FreqItemset<String> tmpItemset : model.freqItemsets().toJavaRDD().collect()) {
                    String str2 = tmpItemset.javaItems().toString().replace("[", "").replace("]", "").replace(" ", "");
                    //System.out.println("Str2:"+str2);
                    long freq2 = itemset.freq();
                    if (str1.length() > str2.length() || str1.equals(str2)) continue;
                    if (isIncluded(str1, freq1, str2, freq2)) {             //若S1是S2的子集，则跳过针对该元素的循环
                        flag = false;
                        break;
                    }

                }
                if (flag) filterResult.add(str1 + "," + freq1);

            }
            filterResult.forEach(System.out::println);
            System.out.println("The size of results:" + filterResult.size());
            return filterResult;

        });

        System.out.println("The example of results:"+dataTmp2.first());






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
            timeDiff = Math.abs((dt2.getTime() - dt1.getTime())/(24*60*60*1000));
            //System.out.println("dt2-dt1="+timeDiff);

            return timeDiff < 1;
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return false;
    }

    private static boolean isIncluded(String s1, long freq1, String s2, long freq2)    //判断S1是否是其他集合的子集
    {

        String[] str1 = s1.split(",");

        for (String aStr1 : str1) {
            if (!s2.contains(aStr1))             //判断S1的元素是否都能在S2中找到
                return false;
        }
        return Objects.equals(freq1, freq2);

    }

}

