javac -classpath `hadoop classpath` -d . WordCount.java
jar cf WordCount.jar WordCount\*.class

Upload Input File
hadoop fs -copyFromLocal 200.txt /user/iitj/


Run the Job
hadoop jar WordCount.jar WordCount /user/iitj/200.txt output


View Results
hadoop fs -getmerge output/ output.txt
cat output.txt
