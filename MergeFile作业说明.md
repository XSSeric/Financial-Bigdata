# MergeFile作业说明

## 环境配置

采用的教程：[MapReduce编程(一)](https://blog.csdn.net/napoay/article/details/68491469?ops_request_misc=&request_id=&biz_id=102&utm_term=idea搭建mapreduce环境&utm_medium=distribute.pc_search_result.none-task-blog-2~all~sobaiduweb~default-1-68491469.142) 、[编程实现文件合并和去重操作](https://blog.csdn.net/gengmeng_/article/details/124677922)

首先在伪分布式系统中安装maven，解压构建软链接，修改配置文件（conf文件夹里的setting.xml，再换成阿里云的源，在系统添加环境变量）

检验安装成功<img src="C:\Users\Eric\AppData\Roaming\Typora\typora-user-images\image-20231024103954477.png" alt="image-20231024103954477" style="zoom:33%;" />

然后安装idea修改配置文件，构建项目MergeFile

修改项目中的依赖与配置并且使其生效

<img src="C:\Users\Eric\AppData\Roaming\Typora\typora-user-images\image-20231024104227005.png" alt="image-20231024104227005" style="zoom:30%;" />

#### 在pom.xml添加依赖

- hadoop-common
- hadoop-hdfs
- hadoop-mapreduce-client-core
- hadoop-mapreduce-client-jobclient
- log4j( 打印日志)

```xml
    <dependencies>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>3.3.6</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>3.3.6</version>
        </dependency>


        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-mapreduce-client-core</artifactId>
            <version>3.3.6</version>
        </dependency>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-mapreduce-client-jobclient</artifactId>
            <version>3.3.6</version>
        </dependency>

        <dependency>
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
            <version>1.2.17</version>
        </dependency>
    </dependencies>
```

#### 添加log4j.properties文件

```properties
log4j.rootLogger = debug,stdout

### ???????? ###
log4j.appender.stdout = org.apache.log4j.ConsoleAppender
log4j.appender.stdout.Target = System.out
log4j.appender.stdout.layout = org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern = [%-5p] %d{yyyy-MM-dd HH:mm:ss,SSS} method:%l%n%m%n
```

## 代码编写

开始尝试学习java并且编写相关类，具体代码和注释在源码中

## 运行结果

9870端口，右上角查看dfs文件目录

<img src="C:\Users\Eric\AppData\Roaming\Typora\typora-user-images\image-20231024103605975.png" alt="image-20231024103605975" style="zoom: 33%;" />

<img src="C:\Users\Eric\AppData\Roaming\Typora\typora-user-images\image-20231024103642635.png" alt="image-20231024103642635" style="zoom: 43%;" />

输入输出文件一并附在MergeFile文件夹中