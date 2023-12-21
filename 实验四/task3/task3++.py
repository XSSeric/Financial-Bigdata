from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# 创建 SparkSession
spark = SparkSession.builder.appName("LoanDefaultPrediction").getOrCreate()

# 加载数据集
data = spark.read.csv("../data/processed_data.csv", header=True, inferSchema=True)

# 特征选择
selected_features = ["AMT_INCOME_TOTAL", "AMT_CREDIT","DAYS_ID_PUBLISH", "FLAG_PHONE", "HOUR_APPR_PROCESS_START", "OBS_60_CNT_SOCIAL_CIRCLE", "DAYS_LAST_PHONE_CHANGE", "FLAG_DOCUMENT_3"] # 根据需要选择特征属性
assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
data = assembler.transform(data)

# 划分训练集和测试集
(trainingData, testData) = data.randomSplit([0.8, 0.2], seed=123)

# 训练模型 - 使用MLlib中的LinearSVC实现
svm = LinearSVC(labelCol="TARGET", featuresCol="features", maxIter=10)
model = svm.fit(trainingData)

# 在测试集上进行预测
predictions = model.transform(testData)

# 评估模型性能
evaluator = BinaryClassificationEvaluator(labelCol="TARGET", metricName="areaUnderROC")  # 使用"areaUnderROC"作为评估指标
auc = evaluator.evaluate(predictions)
print("Area Under ROC:", auc)
# 正确率
accuracy = predictions.filter(predictions.TARGET == predictions.prediction).count() / float(predictions.count())
print("Accuracy:", accuracy)

