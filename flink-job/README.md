# 介绍

大家好，这个仓库里与源库不同的是，这是我在学习flink有名的欺诈系统案例的时候形成的笔记。

笔记的形式有两种：

1. 直接在源代码里添加注释: 这种方式比较直观，方便同学们在阅读代码时辅助理解，不用自己再去搜索。
2. 会加个文件夹或者在别的仓库里发一些关于学习系统并复现的案例与学习经验。

另外想说一点感悟。看代码和自己动手写有质量的代码，是一名好程序员的必经之路。


写代码不够熟练，无他，多学就爽啦。

共勉咯～


# Dynamic Fraud Detection Demo with Apache Flink

## Introduction


### Instructions (local execution with netcat):

1. Start `netcat`:
```
nc -lk 9999
```
2. Run main method of `com.ververica.field.dynamicrules.Main`
3. Submit to netcat in correct format:
rule_id, (rule_state), (aggregation keys), (unique keys), (aggregateFieldName field), (aggregation function), (limit operator), (limit), (window size in minutes)

##### Examples:

1,(active),(paymentType),,(paymentAmount),(SUM),(>),(50),(20)
1,(delete),(paymentType),,(paymentAmount),(SUM),(>),(50),(20)
2,(active),(payeeId),,(paymentAmount),(SUM),(>),(10),(20)
2,(pause),(payeeId),,(paymentAmount),(SUM),(>),(10),(20)

##### Examples JSON:  
{ "ruleId": 1, "ruleState": "ACTIVE", "groupingKeyNames": ["paymentType"], "unique": [], "aggregateFieldName": "paymentAmount", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER","limit": 500, "windowMinutes": 20}

##### Examples of Control Commands:

{"ruleState": "CONTROL", "controlType":"DELETE_RULES_ALL"}
{"ruleState": "CONTROL", "controlType":"EXPORT_RULES_CURRENT"}
{"ruleState": "CONTROL", "controlType":"CLEAR_STATE_ALL"}


##### Examles of CLI params:
--data-source kafka --rules-source kafka --alerts-sink kafka --rules-export-sink kafka

##### Special functions:
1,(active),(paymentType),,(COUNT_FLINK),(SUM),(>),(50),(20)
