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

//编写reduce类
public class DefaultReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    //定义变量result
    private IntWritable result = new IntWritable();
    //编写reduce函数.map的结果经过排序（key字典序）分组，最后每组调用一次reduce函数
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //定义变量sum作为总次数
        int sum = 0;
        //遍历values
        for (IntWritable val : values) {
            //取值
            int val1 = val.get();
            //将val1赋值给sum
            sum += val1;
            //打印reduce的结果
            //System.out.println("<" + key + "," + val1 + ">");
        }
        //将sum赋值给result
        result.set(sum);
        //输出<key,result>
        context.write(key, result);
    }
}
