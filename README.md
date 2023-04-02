<h1 align="center">Hi 👋, I'm Siddhesh Kankal</h1>
<h3 align="center">A passionate data engineer</h3>

# Kafka-project-2-on-Exam-result-streaming-analytics-
- 🔭 I’m currently working on [Kafka-project-2-on-Exam-result-streaming-analytics-](https://github.com/siddheshkankal/Kafka-project-2-on-Exam-result-streaming-analytics-/)

- 👨‍💻 All of my projects are available at [https://github.com/siddheshkankal](https://github.com/siddheshkankal)

- 📝 I regularly write articles on [Data engineering tech stack](Data engineering tech stack)

- 📫 How to reach me **dksidd96@gmail.com**

<h3 align="left">Connect with me:</h3>
<p align="left">
<a href="https://linkedin.com/in/https://www.linkedin.com/in/siddhesh-kankal-bhavsar-20101996" target="blank"><img align="center" src="https://raw.githubusercontent.com/rahuldkjain/github-profile-readme-generator/master/src/images/icons/Social/linked-in-alt.svg" alt="https://www.linkedin.com/in/siddhesh-kankal-bhavsar-20101996" height="30" width="40" /></a>
</p>

<h3 align="left">Languages and Tools:</h3>
<p align="left"> <a href="https://www.docker.com/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/docker/docker-original-wordmark.svg" alt="docker" width="40" height="40"/> </a> <a href="https://hadoop.apache.org/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/apache_hadoop/apache_hadoop-icon.svg" alt="hadoop" width="40" height="40"/> </a> <a href="https://hive.apache.org/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/apache_hive/apache_hive-icon.svg" alt="hive" width="40" height="40"/> </a> <a href="https://pandas.pydata.org/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/2ae2a900d2f041da66e950e4d48052658d850630/icons/pandas/pandas-original.svg" alt="pandas" width="40" height="40"/> </a> <a href="https://www.python.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> </a> </p>

## Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Architecture](#Architecture)
* [Setup](#setup)

## General info
This project is about Student's Exams result streaming Analytics.
	
## Technologies
Project is created with:
* Confluent Kafka
* PySpark
* Python
	
## Architecture

![architecture](https://user-images.githubusercontent.com/60224016/229360163-030b627a-9ed4-46df-8986-f1df65ebd8a9.jpg)



Detailed Explanation: 
In this project we are getting raw data excel (exams.csv) all students meta data along with their marks ,gender,group,reading score ,writing score,
math score,etc..
so through producer code we are producing (or we can say sending data) to kafka topic as this is POC project we have only one node cluster kafka setup
and on the other hand we are consuming the data through consumer code(or we can say subsribing the records) and we have write login as we are consolidating the records based on group wise in first consumer, and simultaneoulsy we can subscribe same topic through another consumer where records are segregated based on pass and fail students within group.
and we are dumping the result set into seperate excel files.


## Setup
To run this project, install it locally:

Then on the Spark shell run the below command from CLI
```
$ Python kafka_producer.py
```
and in another two seperate command prompt 
```
$ Python kafka_consumer.py 
and in another one 
$ Python kafka_consumer2.py
```
