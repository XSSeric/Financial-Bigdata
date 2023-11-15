package task3;
/*
    实验背景：银行贷款违约为背景，任务为根据application_data.csv中的数据，基于MapReduce建立贷款违约检测模型，并评估实验结果的准确率。
    1、该任务可视为一个“二分类”任务，因为数据集只存在两种情况，违约（Class=1）和其他（Class=0）。
    2、可根据时间特征的先后顺序按照8：2的比例将数据集application_data.csv拆分成训练集和测试集，时间小的为训练集，其余为测试集；也可以按照8：2的比例随机拆分数据集。最后评估模型的性能，评估指标可以为accuracy、f1-score等。
    3、基于数据集application_data.csv，可以自由选择特征属性的组合，自行选用分类算法对目标属性TARGET进行预测。
*/
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.Length;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PredictMap extends Mapper<Object, Text, Text, Text> {
    //编写map方法,randomly divide the dataset into training set and test set
    int flag=0;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Text tt = new Text();
        if(flag==0){
            flag=1;
            return;
        }
        //将一行数据转换为String类型
        String line = value.toString();
        //将line按照分隔符“,”进行分割
        String[] split = line.split(",");
        // 使用StringBuilder拼接选定的列
        StringBuilder resultBuilder = new StringBuilder();
        resultBuilder.append(split[59]).append(",").append(split[0]).append(split[40]).append(",").append(split[37])
                .append(",").append(split[36]).append(",").append(split[35]).append(",").append(split[34])
                .append(",").append(split[18]).append(",").append(split[19]).append(",").append(split[20]).append(",")
                .append(split[21]).append(",").append(split[22]).append(",").append(split[23]).append(",").append(split[24]);
        //generate the value out,merge the split of selected columns
        Text result = new Text(resultBuilder.toString());

        //将数据集按照8：2的比例随机拆分成训练集和测试集,generate a random number between 0 and 10,
        //if the number is less than 8,then the data is in the training set,else the data is in the test set
        float random = (float)(Math.random()*10);
        if(random<8) {
            tt.set("train");
        }else {
            tt.set("test");
        }
        context.write(tt, result);
        System.out.println(tt + " " + result);
    }
}
