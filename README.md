# Parallel-FP-Growth
Implementation of Parallel FP Growth Algorithm on a Hadoop Cluster for mining frequent itemsets from a large transactional Database using Map Reduce Programming

More can be found at - https://en.wikibooks.org/wiki/Data_Mining_Algorithms_In_R/Frequent_Pattern_Mining/The_FP-Growth_Algorithm

# USER GUIDE
Upload Input Data to HDFS:

> hdfs dfs -mkdir -p input

> hdfs dfs -put dataSet/sample input/

Use command as follow:
To compile-

>./run.sh  

To run-

hadoop jar bigFIM.jar bigFIM.BigFIMDriver datasetName, minsup(relative), input_file, output_file, datasize, childJavaOpts, nummappers, prefix_length
e.g.

>hadoop jar FpGrowth.jar main.FpGrowth sample 0.5 input output 6 2048 1 2

>hadoop jar FpGrowth.jar main.FpGrowth mushroom 0.1 input output 8124 2048 1 3

Output summary will be generated as XXX_ResultOut (e.g. FPGrowth_mushroom_ResultOut)

Extract final output/raw data from HDFS :

hdfs dfs -copyToLocal output/ag/part-r-00000  .