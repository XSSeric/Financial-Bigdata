"""
编写Spark程序，统计application_data.csv中客户贷款金额AMT_CREDIT 比客户收入AMT_INCOME_TOTAL差值最高和最低的各十条记录。
输出格式：
<SK_ID_CURR><NAME_CONTRACT_TYPE><AMT_CREDIT><AMT_INCOME_TOTAL>, <差值>,差值=AMT_CREDIT-AMT_INCOME_TOTAL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, row_number
from pyspark.sql.window import Window

# 创建 SparkSession
spark = SparkSession.builder.appName("MaxMinCreditIncomeDifference").getOrCreate()

# 读取 CSV 文件
file_path = "../data/application_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 计算差值并按差值排序
diff_df = df.withColumn("Difference", col("AMT_CREDIT") - col("AMT_INCOME_TOTAL")) \
    .select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "Difference") \
    .withColumn("rank_max", row_number().over(Window.orderBy(col("Difference").desc()))) \
    .withColumn("rank_min", row_number().over(Window.orderBy(col("Difference"))))

# 找出差值最高和最低的十条记录
max_diff = diff_df.filter(col("rank_max") <= 10).select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "Difference")
min_diff = diff_df.filter(col("rank_min") <= 10).select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "Difference")

# 显示结果
max_diff.show(truncate=False)
min_diff.show(truncate=False)

# 输出文件
output_file_path = "../output/task1_2"  # 设置输出文件路径
max_diff.write.csv(output_file_path, header=True)
min_diff.write.csv(output_file_path, header=True, mode="append")