1. Compile the Java Code:
   
   javac -classpath $(hadoop classpath) -d . SumReducer.java
2. Create a JAR file:
   
   jar -cvf SumReducer.jar \*.class
3. Run Hadoop Job:
   
   hadoop jar SumReducer.jar SumReducer /input/path /output/path
