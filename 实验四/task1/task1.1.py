from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, concat, lit
from pyspark.sql.types import StringType

# 创建 SparkSession
spark = SparkSession.builder.appName("LoanAmountDistribution").getOrCreate()

# 读取 CSV 文件
file_path = "../data/application_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 计算贷款金额范围的分布情况
loan_distribution = df.withColumn("LoanRange", (floor(col("AMT_CREDIT") / 10000) * 10000)) \
    .groupBy("LoanRange") \
    .count() \
    .orderBy("LoanRange")

# 格式化输出的函数
def format_output(start_range, count):
    end_range = start_range + 10000
    return f"(({start_range},{end_range}),{count})"

# 注册UDF
format_output_udf = spark.udf.register("format_output", format_output, StringType())

# 应用格式化输出函数到每一行
formatted_distribution = loan_distribution.withColumn("FormattedOutput", format_output_udf("LoanRange", "count"))

# 选取需要的列并显示结果
formatted_distribution.select("FormattedOutput").show(truncate=False)

# 将结果保存到CSV文件
output_file_path = "../output/task1_1"  # 设置输出文件路径
formatted_distribution.select("FormattedOutput").write.csv(output_file_path, header=True)