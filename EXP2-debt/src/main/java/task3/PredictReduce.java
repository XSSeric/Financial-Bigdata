package task3;
/*
    实验背景：银行贷款违约为背景，任务为根据application_data.csv中的数据，基于MapReduce建立贷款违约检测模型，并评估实验结果的准确率。
    1、该任务可视为一个“二分类”任务，因为数据集只存在两种情况，违约（Class=1）和其他（Class=0）。
    2、可根据时间特征的先后顺序按照8：2的比例将数据集application_data.csv拆分成训练集和测试集，时间小的为训练集，其余为测试集；也可以按照8：2的比例随机拆分数据集。最后评估模型的性能，评估指标可以为accuracy、f1-score等。
    3、基于数据集application_data.csv，可以自由选择特征属性的组合，自行选用分类算法对目标属性TARGET进行预测。
*/
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.Length;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.AbstractMap;
import java.util.Map.Entry;

import task2.WeekdayReduce;

public class PredictReduce extends Reducer<Text, Text, Text, Text>{

    // 创建训练集列表，每个元素是一个整数列表
    protected List<List<Integer>> train = new ArrayList<>();
    // 创建测试集列表，每个元素是一个整数列表
    protected List<List<Integer>> test = new ArrayList<>();


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println(key);
        // the sum of train&test
//        Text res = new Text();
//        int sum = 0;
//        for (Text value : values) {
//            System.out.println(value);
//            sum = sum + 1;
//        }
        //generate the train set and test set

        if(key.toString().equals("train")) {
            for (Text value : values) {
                String[] split = value.toString().split(",");
                List<Integer> temp = new ArrayList<>();
                for (int i = 0; i < split.length; i++) {
                    temp.add(Integer.parseInt(split[i]));
                }
                train.add(temp);
            }
        }else {
            for (Text value : values) {
                String[] split = value.toString().split(",");
                List<Integer> temp = new ArrayList<>();
                for (int i = 0; i < split.length; i++) {
                    temp.add(Integer.parseInt(split[i]));
                }
                test.add(temp);
            }
        }
        //打印train和test的元素数量
        System.out.println(train.size());
        System.out.println(test.size());
        System.out.println("------------------------------------");
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 使用 KNN 进行分类
        int k = 3; // 设置 k 值
        int m = 0;
        int oo = 0;
        int oi = 0;
        int io = 0;
        int ii = 0;
        for (List<Integer> testInstance : test) {
            String predictedLabel = knnClassify(train, testInstance, k);
            //System.out.println(predictedLabel);
            //System.out.println(testInstance.get(0));
            System.out.println(m);
            m++;
            if(m>500) break;
            //将预测结果转换为int
            int predictedLabelInt = Integer.parseInt(predictedLabel);
            //比较预测结果和真实结果
            if(predictedLabelInt == 0 && testInstance.get(0)==0) {
                oo++;
                context.write(new Text(testInstance.get(1).toString()), new Text("Right"));
                System.out.println("OKOKOK 1");
            }
            if(predictedLabelInt == 0 && testInstance.get(0)==1) {
                oi++;
                context.write(new Text(testInstance.get(1).toString()), new Text("Wrong"));
                System.out.println("OKOKOK 2");
            }
            if(predictedLabelInt == 1 && testInstance.get(0)==0) {
                io++;
                context.write(new Text(testInstance.get(1).toString()), new Text("Wrong"));
                System.out.println("OKOKOK 3");
            }
            if(predictedLabelInt == 1 && testInstance.get(0)==1) {
                ii++;
                context.write(new Text(testInstance.get(1).toString()), new Text("Right"));
                System.out.println("OKOKOK 4");
            }
        }
        //计算准确率
        float accuracy = (float)(oo+ii)/(oo+oi+io+ii);
        context.write(new Text("Accuracy"), new Text(String.valueOf(accuracy)));
        //计算精确率
        float precision = (float)(ii)/(ii+io);
        context.write(new Text("Precision"), new Text(String.valueOf(precision)));
        //计算召回率
        float recall = (float)(ii)/(ii+oi);
        context.write(new Text("Recall"), new Text(String.valueOf(recall)));
        //计算F1-score
        float f1 = (float)(2*precision*recall)/(precision+recall);
        context.write(new Text("F1-score"), new Text(String.valueOf(f1)));
    }

    // KNN 分类算法
    private String knnClassify(List<List<Integer>> trainSet, List<Integer> testInstance, int k) {
        // 存储距离及其对应的标签
        List<Map.Entry<Integer, Integer>> distances = new ArrayList<>();

        // 计算测试实例与训练集中每个实例的距离
        for (List<Integer> trainInstance : trainSet) {
            int distance = calculateEuclideanDistance(testInstance, trainInstance);
            // 创建键值对并保存到distance里面
            Map.Entry<Integer, Integer> entry = new AbstractMap.SimpleEntry<>(distance, trainInstance.get(0));
            distances.add(entry); // 保存距离及其对应的标签
        }

        // 对距离进行排序
        distances.sort(Map.Entry.comparingByKey());

        // 统计前 k 个邻居的标签
        Map<Integer, Integer> labelCounts = new HashMap<>();
        for (int i = 0; i < k; i++) {
            int label = distances.get(i).getValue();
            labelCounts.put(label, labelCounts.getOrDefault(label, 0) + 1);
        }

        // 找到最常见的标签
        int maxCount = 0;
        int predictedLabel = -1;
        for (Map.Entry<Integer, Integer> entry : labelCounts.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                predictedLabel = entry.getKey();
            }
        }

        return String.valueOf(predictedLabel);
    }

    // 计算两个实例之间的欧几里得距离
    private int calculateEuclideanDistance(List<Integer> instance1, List<Integer> instance2) {
        int sum = 0;
        for (int i = 2; i < instance1.size(); i++) { // 0列是标签，1列是id,不计算在内
            int diff = instance1.get(i) - instance2.get(i);
            sum += diff * diff;
        }
        return (int) sum;
    }

//    // 打印数据
//    private void printData(List<List<Integer>> data) {
//        for (List<Integer> instance : data) {
//            System.out.println(instance);
//        }
//    }
}
