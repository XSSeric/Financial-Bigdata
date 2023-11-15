package task2;
/*
    实验背景：银行贷款违约为背景，任务二为编写mapreduce程序，统计日期并按照数量多少排序
    按照数量进行输出，输出格式为<weekday><交易数量>，如Sunday 16000
*/
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.Length;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class WeekdayReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
    //引入log
    private static final Log Log = LogFactory.getLog(WeekdayReduce.class);
    //定义TreeMap，按照value排序，最后作为结果遍历输出
    private TreeMap<Integer, Text> resultTreeMap = new TreeMap<>(Collections.reverseOrder());

    //编写reduce函数.map的结果经过排序（key字典序）分组，最后每组调用一次reduce函数
    //这道题reduce用来生成一颗有序树，树的节点是<交易数量，weekday>

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //定义变量sum作为总次数
        //System.out.println("reduce");
        int sum = 0;
        //遍历values
        for (IntWritable val : values) {
            //聚合相同的一类
            int val1 = val.get();
            sum += val1;
            //System.out.println(sum);
        }
        //System.out.println(sum);
        resultTreeMap.put(sum, new Text(key));
        //Log.info("reduce"+sum);
    }

    //在最后的收尾函数输出树
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, Text> entry : resultTreeMap.entrySet()) {
            context.write(entry.getValue(), new IntWritable(entry.getKey()));
        }
    }

}
