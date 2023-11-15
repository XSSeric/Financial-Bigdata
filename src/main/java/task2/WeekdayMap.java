package task2;
/*
    实验背景：银行贷款违约为背景，任务二为编写mapreduce程序，统计日期并按照数量多少排序
    按照数量进行输出，输出格式为<weekday><交易数量>，如Sunday 16000
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

public class WeekdayMap extends Mapper<Object, Text, Text, IntWritable>{
    //定义变量word，作为输出键
    private Text day = new Text();
    //定义变量one，作为输出值
    private final static IntWritable one = new IntWritable(1);

    //编写map方法,统计星期的数量，读到一个就返回一个<day,one>
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //将一行数据转换为String类型
        String line = value.toString();
        //将line按照分隔符“,”进行分割
        String[] split = line.split(",");
        if(split[25].equals("MONDAY") || split[25].equals("TUESDAY") || split[25].equals("WEDNESDAY") || split[25].equals("THURSDAY") || split[25].equals("FRIDAY") ||
                split[25].equals("SATURDAY") || split[25].equals("SUNDAY")) {
            //将日期赋值给输出键
            day.set(split[25]);
            //输出<word,one>
            context.write(day, one);
            //打印map的结果
            //System.out.println("<" + day + "," + one + ">");
        }
    }
}
