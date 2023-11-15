package task1;
/*
    实验背景：银行贷款违约为背景，任务一为编写mapreduce程序，统计数据集中违约和不违约的数量
    按照标签target进行输出，输出格式为<标签><交易数量>，如1 100
*/
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DefaultDriver {
    //编写main方法
    public static void main(String[] args) throws Exception {
        //生成配置实例
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://localhost:9000");

        //传递参数作为路径
        String[] Args = new String[]{"input/task1","output/task1"};
        if (Args.length != 2) {
            //输出错误信息退出程序
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }

        //实例化job，指定各种组件属性和输入输出类型。输入输出路径。map和reduce的实例。
        Job job = Job.getInstance(conf, DefaultDriver.class.getSimpleName());
        //设置jar包
        job.setJarByClass(DefaultDriver.class);
        //设置map类以及输出键值对类型
        job.setMapperClass(DefaultMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置combine和reduce类以及输出键值对类型
        job.setCombinerClass(DefaultReduce.class);
        job.setReducerClass(DefaultReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(Args[0]));
        FileOutputFormat.setOutputPath(job, new Path(Args[1]));
        //退出程序
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
