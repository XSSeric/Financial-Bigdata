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

public class PredictDriver {
    //编写main方法
    public static void main(String[] args) throws Exception {
        //生成配置实例
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://localhost:9000");

        //传递参数作为路径
        String[] Args = new String[]{"input/task3","output/task3"};
        if (Args.length != 2) {
            //输出错误信息退出程序
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }

        //实例化job，指定各种组件属性和输入输出类型。输入输出路径。map和reduce的实例。
        Job job = Job.getInstance(conf, PredictDriver.class.getSimpleName());
        //设置jar包
        job.setJarByClass(PredictDriver.class);
        //设置map类以及输出键值对类型
        job.setMapperClass(PredictMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置combine和reduce类以及输出键值对类型
        //job.setCombinerClass(PredictReduce.class);
        job.setReducerClass(PredictReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(Args[0]));
        FileOutputFormat.setOutputPath(job, new Path(Args[1]));
        //退出程序
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
