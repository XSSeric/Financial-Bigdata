# 金融大数据处理课程项目

这个仓库将会陆续发布关于金融大数据处理课程的项目，项目详情请参照每个。
- MergeFile是一个小型的Java程序，使用Hadoop来合并两个文件。其中input是由两个excel文件转换而来的txt文件，output是合并后的文件。第⼀列为三⼤指数编号（简化为101、102、103），第⼆列为成分股代码。

## 使用需求

- Java 8或更高版本
- Hadoop 2.7.3或更高版本

## 使用方法

1. 克隆仓库：`git clone https://github.com/your-username/financial-big-data-processing.git`
2. 构建项目：`mvn clean package`
3. 运行程序：`hadoop jar target/financial-big-data-processing-1.0-SNAPSHOT.jar input1.txt input2.txt output`

程序将合并`input1.txt`和`input2.txt`，并将结果写入`output`目录。

## 贡献

欢迎贡献！如果您想为此项目做出贡献，请提交拉取请求。

## 许可证

本项目基于MIT许可证发布。有关详细信息，请参见[LICENSE](LICENSE)文件。
