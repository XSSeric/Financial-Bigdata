"""
统计每个客户出生以来每天的平均收入（avg_income）=总收入（AMT_INCOME_TOTAL）/出生天数（DAYS_BIRTH)，统计每日收入大于1的客户，并按照从大到小排序，保存为csv。
输出格式：<SK_ID_CURR>, <avg_income>
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 创建 SparkSession
spark = SparkSession.builder.appName("DailyIncome").getOrCreate()

# 读取 CSV 文件
file_path = "../data/application_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 计算每个客户出生以来每天的平均收入（avg_income）
df = df.withColumn("avg_income", col("AMT_INCOME_TOTAL") / (-col("DAYS_BIRTH")))

# 筛选出每日收入大于1的客户，并按照从大到小排序
filtered_df = df.filter(col("avg_income") > 1).select("SK_ID_CURR", "avg_income").orderBy(col("avg_income").desc())

# 保存为 CSV 文件
output_file_path = "../output/task2_2"  # 设置输出文件路径
filtered_df.write.csv(output_file_path, header=True)
