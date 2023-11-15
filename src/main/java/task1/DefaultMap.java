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
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.Length;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//编写map类
public class DefaultMap extends Mapper<Object, Text, Text, IntWritable> {
    //定义变量one，作为输出值
    private final static IntWritable one = new IntWritable(1);
    //定义变量word，作为输出键
    private Text word = new Text();
    //编写map方法，是核心逻辑。被调用的次数和输入的kv对有关。读取一行调用一次。
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //将一行数据转换为String类型
        String line = value.toString();
        //将line按照分隔符“,”进行分割
        String[] split = line.split(",");
        if(split[split.length-1].equals("1") || split[split.length-1].equals("0")) {
            //将所需要的违规列的值赋值给变量target
            String target = split[59];
            //将target赋值给变量word
            word.set(target);
            //输出<word,one>
            context.write(word, one);
            //打印map的结果
            //System.out.println("<" + word + "," + one + ">");
        }
    }
}
//map结束后输出是<1,1><0,1>...