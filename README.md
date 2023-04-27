# Real-Time Churn Prediction System for Telecom Companies

This project is a proof-of-concept system that uses Kafka, Apache Spark, and H2O GBM model to predict churn in real-time for a telecom company. The system is divided into three modules: a publisher module that sends customer data to a Kafka topic, a consumer module that reads data from the Kafka topic and writes it to an Apache Spark stream, and a third module that reads from the Apache Spark stream and uses an H2O GBM model to predict churn rate.

The project was divided into two stages. In the first stage, a classification model was trained on a public telecom dataset from Kaggle. This model was used to predict customer churn based on features such as call duration, call frequency, and plan type. In the second stage, a Kafka service was configured on the Upstash Kafka platform, and the Kafka producer, consumer, and H2O integration with Apache Spark stream were designed and implemented.

## Getting Started


You will need to download the public telecom dataset from Kaggle and place it in the data folder (https://www.kaggle.com/datasets/blastchar/telco-customer-churn).

To run the system, follow these steps:

1. Create an account on Upstash Kafka and create a topic
2. Create a Databricks community account
3. Import all .py files as Source to be converted to databricks notebooks
4. Train the model running the training notebook
5. Run the publisher module to send customer data to the Kafka topic
6. Run the third module to predict churn rate in real-time



## Conclusion

This real-time churn prediction system has significant potential to benefit the telecom industry by providing a powerful tool for predicting and managing churn. With further development and refinement, this system could become a valuable tool for companies looking to improve customer retention and increase profitability.
