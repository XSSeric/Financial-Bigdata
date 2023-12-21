"""
1、统计所有男性客户（CODE_GENDER=M）的小孩个数（CNT_CHILDREN）类型占比情况。输出格式为：
<CNT_CHILDREN>，<类型占比>
例：0，0.1234表示没有小孩的男性客户占总男性客户数量的占比为0.1234。
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 创建 SparkSession
spark = SparkSession.builder.appName("ChildrenCountRatio").getOrCreate()

# 读取 CSV 文件
file_path = "../data/application_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 筛选出男性客户
male_customers = df.filter(col("CODE_GENDER") == "M")

# 统计男性客户中小孩个数的类型占比
total_male_customers = male_customers.count()
children_count = male_customers.groupBy("CNT_CHILDREN").count().orderBy("CNT_CHILDREN")

# 计算类型占比
children_count_ratio = children_count.withColumn("Ratio", col("count") / total_male_customers).select("CNT_CHILDREN", "Ratio")

# 显示结果
children_count_ratio.show(truncate=False)

# 输出文件
output_file_path = "../output/task2_1"  # 设置输出文件路径
children_count_ratio.write.csv(output_file_path, header=True)