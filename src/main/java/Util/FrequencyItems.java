package Util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.util.*;

/**
 * @author BO QIU
 */


public class FrequencyItems {

    /**
     * @param itemFrequency 判定频次
     * @param inputItems    输入的集合列表
     * @param type          频次的逻辑类型   {0：大于  1：小于  2：等于  3：小于等于  4：大于等于}
     * @param id            所查人的id
     * @return Set<String> 将所有频繁项集的结果以一个集合的形式给出</String>
     */
    public static Set<List<String>> getFrequencyItems(JavaRDD<Set<List<String>>> inputItems, int itemFrequency, int type, List<String> id) throws IllegalArgumentException {

        long recordNumber = inputItems.count();
        int itemF;

        if (recordNumber == 0)
            throw new IllegalArgumentException("getFrequencyItems()---输入集合的个数不能为零");
        if (itemFrequency < 2)
            throw new IllegalArgumentException("getFrequencyItems()---输入判定次数不能小于2");
        if (itemFrequency > recordNumber)
            throw new IllegalArgumentException("getFrequencyItems()---输入判定次数不能大于记录数量");

        switch (type) {
            case 0:
                itemF = itemFrequency + 1;
                break;
            case 2:
            case 4:
                itemF = itemFrequency;
                break;
            default:
                itemF = 2;
                break;
        }

        System.out.println("The minSupport is: " + (double) itemFrequency / recordNumber);
        FPGrowth fpg = new FPGrowth()
                .setMinSupport((double) itemF / recordNumber)                              //最小支持度
                .setNumPartitions(10);
        FPGrowthModel<List<String>> model = fpg.run(inputItems);
        List<List<String>> resultList = model.freqItemsets().toJavaRDD().filter(l -> l.javaItems().size() == 2 && l.javaItems().contains(id)).filter((Function<FPGrowth.FreqItemset<List<String>>, Boolean>) listFreqItemset -> {
            switch (type) {
                case 1:
                    return listFreqItemset.freq() < itemFrequency;
                case 3:
                    return listFreqItemset.freq() <= itemFrequency;
                default:
                    return true;
            }
        }).flatMap(new FlatMapFunction<FPGrowth.FreqItemset<List<String>>, List<String>>() {
            private static final long serialVersionUID = -5607387952474606920L;

            @Override
            public Iterator<List<String>> call(FPGrowth.FreqItemset<List<String>> strings) throws Exception {
                return (strings.javaItems()).iterator();
            }
        }).distinct().collect();


        //System.out.println("The model.freqItemsets: "+model.freqItemsets().toJavaRDD().collect());

//        List<List<List<String>>> resultList = model.freqItemsets().toJavaRDD().filter(l -> l.javaItems().size()==2 && l.javaItems().contains(id)).filter((Function<FPGrowth.FreqItemset<List<String>>, Boolean>) listFreqItemset -> {
//            switch (type)
//            {
//                case 1:
//                    return listFreqItemset.freq()<itemFrequency ;
//                case 3:
//                    return listFreqItemset.freq()<=itemFrequency ;
//                default:
//                    return true;
//            }
//        }).map(FPGrowth.FreqItemset::javaItems).distinct().collect();


        return new HashSet<>(resultList);

    }

    public static List<Set<String>> removeSubSet(List<FPGrowth.FreqItemset<String>> result) {

        List<Set<String>> filterResult = new ArrayList<>();
        for (FPGrowth.FreqItemset<String> itemset : result) {
            boolean flag = true;
            Set<String> set1 = new HashSet<>();
            long freq1 = itemset.freq();
            set1.addAll(itemset.javaItems());

            for (FPGrowth.FreqItemset<String> tmpItemset : result) {
                Set<String> set2 = new HashSet<>();
                long freq2 = tmpItemset.freq();
                set2.addAll(tmpItemset.javaItems());

                if (set2.containsAll(set1) && !set1.equals(set2) && Objects.equals(freq1, freq2)) {             //若S1是S2的子集，则跳过针对该元素的循环
                    flag = false;
                    break;
                }
            }
            if (flag) filterResult.add(set1);

        }
        return filterResult;
    }
}
