from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# 创建 SparkSession
spark = SparkSession.builder.appName("LoanDefaultPrediction").getOrCreate()

# 加载数据集
data = spark.read.csv("../data/application_data.csv", header=True, inferSchema=True)

# 特征选择
selected_features = ["AMT_CREDIT", "HOUR_APPR_PROCESS_START", "REGION_RATING_CLIENT_W_CITY", "FLAG_DOCUMENT_3", "FLAG_DOCUMENT_7", "CNT_CHILDREN", "AMT_INCOME_TOTAL"] # 根据需要选择特征属性
assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
data = assembler.transform(data)

# 划分训练集和测试集
(trainingData, testData) = data.randomSplit([0.8, 0.2], seed=123)

# 训练模型 - 使用决策树分类器
dt = DecisionTreeClassifier(labelCol="TARGET", featuresCol="features")
model = dt.fit(trainingData)

# 在测试集上进行预测
predictions = model.transform(testData)

# 评估模型性能
evaluator = BinaryClassificationEvaluator(labelCol="TARGET", metricName="areaUnderROC")  # 使用"areaUnderROC"作为评估指标
auc = evaluator.evaluate(predictions)
print("Area Under ROC:", auc)
# 正确率
accuracy = predictions.filter(predictions.TARGET == predictions.prediction).count() / float(predictions.count())
print("Accuracy:", accuracy)
# 查准率
precision = predictions.filter(predictions.prediction == 0).filter(predictions.prediction == predictions.TARGET).count() / float(predictions.filter(predictions.prediction == 1).count())
print("Precision:", precision)
# 查全率
recall = predictions.filter(predictions.prediction == 0).filter(predictions.prediction == predictions.TARGET).count() / float(predictions.filter(predictions.TARGET == 1).count())
print("Recall:", recall)
# F1值
F1 = 2 * precision * recall / (precision + recall)
print("F1:", F1)
