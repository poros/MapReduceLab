MapReduceLab
============

Solutions of the Hadoop Map Reduce Lab for the course of Distributed Systems And Cloud Computing

Our answers to the questions addressed in this document are available at

https://github.com/poros/MapReduceLab/blob/master/mapreduce-lab/Answers.md

Below the main wiki page of the laboratory as it was in the repository of the course. 

https://github.com/michiard/CLOUDS-LAB/wiki/MapReduce-Lab

# MapReduce Laboratory

In this laboratory students will learn how to use the [hadoop][hadoop] client API by working on a series of exercises: 

+ The classic Word Count and variations on the theme
+ Design Pattern: Pair and Stripes
+ Design Pattern: Order Inversion 
+ Join implementations

Note that the two design patterns outlined above have been originally discussed in:

+ Jimmy Lin, Chris Dyer, **Data-Intensive Text Processing with MapReduce**, Morgan Claypool ed.  

The following [Link][cheatsheet] contains a "cheat-sheet" to help students with common commands on Hadoop.


[cheatsheet]: https://github.com/michiard/CLOUDS-LAB/blob/master/C-S.md "Cheatsheet"
[hadoop]: http://hadoop.apache.org "hadoop"

## Setup the laboratory sources in Eclipse:

The first step is to import the laboratory source code into Eclipse.

+ Download the project file [mapred-lab.jar][mapred-lab.jar]
+ Open Eclipse and select File -> Import... -> Existing Projects into Workspace
+ From the Import Projects dialog window, choose Select archive file and then Browse... to import mapred-lab.jar that you downloaded at step 1
+ Select the project mapred-lab and the press the Finish button

At this point you should have a java project named mapred-lab already configured that compiles. The next step is starting with the first exercise.

[mapred-lab.jar]: https://github.com/michiard/CLOUDS-LAB/raw/master/mapreduce-lab/mapred-lab.jar "mapred-lab.jar"

## How to launch the jobs: <a id="runjob"></a>

In order to run your code, you have to connect to a Client server using SSH.
You should have received your account details by mail (check your Eurecom email).

In order to connect to the client machine, type:

```
ssh <username>@192.168.45.14
```

If you need to copy any file to/from the client machine, type:

```
scp myfiles <username>@192.168.45.14:~/
scp <username>@192.168.45.14:~/output ./
```

Note that you need to copy the exported jars to the client in order to run the jobs.
```
scp myjar.jar <username>@192.168.45.14:~/
```

## Web interfaces: monitor job progress:

Hadoop publishes some web interfaces that display JobTracker and HDFS statuses.
In order to access them, you need to create a dynamic SSH tunnel and configure your browser to use a SOCKS proxy.
On your machine, type:

```
ssh -N -f -D 1081 <username>@192.168.45.14
```

Then, set up the SOCKS proxy in your browser, using localhost:1081 as address:
in Firefox, go to Preferences -> Advanced -> Network -> Connection Settings and use 127.0.0.1 1080

Now, you can access the Hadoop web interfaces using the following links:

- Jobtracker Web Interface: http://10.10.12.43:50030/
- NameNode Web Interface: http://10.10.12.43:50070/ 

# Exercises
Note, exercises are organized in ascending order of difficulty. An high level overview of this laboratory (excluding the exercises on joins) is available at this [Link][mr-lab]

[mr-lab]: http://www.eurecom.fr/~michiard/teaching/slides/clouds/mr-lab.pdf "Algorithm Design"

## EXERCISE 1:: Word Count

Count the occurrences of each word in a text file. This is the simplest example of MapReduce job: in the following we illustrate three possible implementations.

+ **Basic**: the map method emits 1 for each word. The reduce aggregates the ones it receives from mappers for each key it is responsible for and save on disk (HDFS) the result
+ **In-Memory Combiner**: instead of emitting 1 for each encountered word, the map method (partially) aggregates the ones and emit the result for each word
+ **Combiner**: the same as In-Memory Combiner but using the MapReduce Combiner class


### Instructions

For the **basic** version the student has to modify the file *WordCount.java* in the package *fr.eurecom.dsg.mapreduce*. The user must operate on each TODO filling the gaps with code, following the description associated to each TODO. The package *java.util* contains a class *StringTokenizer* that can be used to tokenize the text.

For The **In-Memory** Combiner and the **Combiner** the student has to modify *WordCountIMC.java* and *WordCountCombiner.java* in the same package referenced aboce. The student has to operate on each TODO using the same code of the basic word count example except for the TODOs marked with a star *. Those must be completed with using the appropriate design pattern.

When an execise is completed you can export it into a jar file that will then be used to execute it.

### Example of usage
The final version should get in input three arguments: the number of reducers, the input file and the output path. Example of execution are:

```
hadoop jar <compiled_jar> fr.eurecom.dsg.mapreduce.WordCount 3 <input_file> <output_path>
hadoop jar <compiled_jar> fr.eurecom.dsg.mapreduce.WordCountIMC 3 <input_file> <output_path>
hadoop jar <compiled_jar> fr.eurecom.dsg.mapreduce.WordCountCombiner 3 <input_file> <output_path>
```

To test your code use the file <a href="https://github.com/michiard/CLOUDS-LAB/tree/master/mapreduce-lab/inputs/quote">quote</a>. Expected result in the file <a href="https://github.com/michiard/CLOUDS-LAB/tree/master/mapreduce-lab/outputs/wordcount.out">wordcount quote result</a>.

To run your job in a realistic input, use the following input file on HDFS of the BigFoot cluster: ```/data/mumak.log```

Note: in order to launch the job, refer to [How to launch the jobs](#runjob)

### Questions ###

Answer the following questions (in a simple text file):

+ do you think the number of reducers affects word count? Try with different values and substantiate your answer
+ use the JobTracker web interface to examinate the job counters: can you explain the differences among all the implementations? For example, look at the amount of bytes shuffled by Hadoop
+ some words of the dictionary are more used than others and reducers associated with those words will work more than the others. Do you think this is good? Why?

## Design Pattern: Stripes (Design Pattern)
Stripes are used to put inside a value a Java <a href="http://docs.oracle.com/javase/6/docs/api/java/util/Map.html">Map</a> that can be serialized and deserialized. In this exercise the student will understand how to create a custom Hadoop data type to be used as value type.

The exercise consists on counting the co-occurrences of every ordered pair of sequential words in the text appearing in the same line. To be more specific, the word *w1* co-occurs with the word *w2* if the two words are in the same line and the first word is immediately followed by the second one. For example, the text "UNIX is simple" contains the pairs ("UNIX", "is") and ("is", "simple").

The key in this exercise is the word on the left of the pair of words. The value is a map containing the words on the right of the pair and the number of occurrences of that pair. For example, the text "UNIX  is simple, UNIX is easy" should output two records: ("UNIX", { "is": 2}) and ("is", {"simple":1, "easy":1}) where { } is the symbol for map.

### Instructions
There are two files for this exercise:

+ *StringToIntMapWritable.java*: the data structure file, to be implemented (in the old version this file is called *StringToIntAssociativeArray.java*)
+ *Stripes.java*: the MapReduce job, that the student must implement using the StringToIntMapWritable data structure

### Example of usage
```
hadoop jar <compiled_jar> fr.eurecom.dsg.mapreduce.Stripes 2 <input_file> <output_path>
```
To test your code use the file <a href="https://github.com/michiard/CLOUDS-LAB/tree/master/mapreduce-lab/inputs/quote">quote</a>.
Expected result in the file <a href="https://github.com/michiard/CLOUDS-LAB/tree/master/mapreduce-lab/outputs/quote.stripes.out">stripes quote result</a>.

### Questions ###
Answer the following questions (in a simple text file):

+ Do you think the reducer of stripes can be used as combiner?
+ Do you think Stripes could be used with the in-memory combiner pattern?
+ How does the number of reducer influence the behavior of the Stripes approach?

Note: in order to launch the job, refer to [How to launch the jobs](#runjob)

## EXERCISE 3:: Pair (Design Pattern)

Use a composite key to emit an occurrence of a pair of words. In this exercise the student will understand how to create a custom Hadoop data type to be used as key type.

A Pair is a tuple composed by two elements that can be used to ship two objects within a partent object. For this exercise the student has to implement a TextPair, that is a Pair that contains two words.

### Instructions
There are two files for this exercise:

+ *TextPair.java*: data structure to be implemented by the student. Besides the implementation of the data structure itself, the student has to implement the serialization Hadoop API (write and read Fields).
+ *Pair.java*: the implementation of a pair example using *TextPair.java* as datatype.

### Example of usage
The final version should get in input three arguments: the number of reducers, the input file and the output path. Example of execution are:
```
hadoop jar <compiled_jar> fr.eurecom.dsg.mapreduce.Pair 1 <input_file> <output_path>
```
To test your code use the file <a href="https://github.com/michiard/CLOUDS-LAB/tree/master/mapreduce-lab/inputs/quote">quote</a>.
Expected result in the file <a href="https://github.com/michiard/CLOUDS-LAB/tree/master/mapreduce-lab/outputs/quote.pair.out">pair quote result</a>.

### Questions ###
Answer the following questions (in a simple text file):

+ How does the number of reducer influence the behavior of the Pairs approach?
+ Compared to the Stripes exercise, for a given number of reducer, how many bytes are shuffled?

Note: in order to launch the job, refer to [How to launch the jobs](#runjob)

## EXERCISE 4:: Order Inversion (Design Pattern)
Order inversion is a design pattern used for computing relative frequencies of word occurences. What is the number of occourrence of a pair containing a word as first element related to the number of occourrence of that word. For instance, if the word "dog" followed by the word "cat" occours 10 times and the word "dog" occours 20 times in total, we say that the frequency of the pair ("dog","cat") is 0.5. The student has to implement the map and reduce methods and the special partitioner (see OrderInversion.PartitionerTextPair class) that permits to send all data about a particular word to a single reducer. Note that inside the OrderInversion class there is a field called ASTERISK which should be used to output the total number of occourrences of a word. Refer to the laboratory slides for more information.

### Instructions
There is one file for this exercise called *OrderInversion.java*. The run method of the job is already implemented, the student should program the mapper, the reducer and the partitioner (see the TODO).

### Example of usage
```
hadoop jar <compiled_jar> fr.eurecom.fr.mapreduce.OrderInversion 4 <input_file> <output_path>
```

To test your code use the file <a href="https://github.com/michiard/CLOUDS-LAB/tree/master/mapreduce-lab/inputs/quote">quote</a>.
Expected result in the file <a href="https://github.com/michiard/CLOUDS-LAB/tree/master/mapreduce-lab/inputs/oi_quote.out">order inversion quote result</a>.

### Questions ###
Answer the following questions (in a simple text file). In answering the questions below, consider the role of the combiner.

+ Do you think the Order Inversion approach is 'faster' than a naive approach with multiple jobs? For example, consider implementing a compound job in which you compute the numerator and the denominator separately, and then perform the computation of the relative frequency
+ What is the impact of the use of a 'special' compound key on the amounts of shuffled bytes ?

Note: in order to launch the job, refer to [How to launch the jobs](#runjob)

## EXERCISE 5:: Joins
In MapReduce the term join refers to merging two different dataset stored as unstructured files in HDFS. As for databases, in MapReduce there are many different kind of joins, each with its use-cases and constraints. In this laboratory the student will implement two different kinds of MapReduce join techniques:

+ **Distributed Cache Join**: this join technique is used when one of the two files to join is small enough to fit (eventually in memory) on each computer of the cluster. This file is copied locally to each computer using the Hadoop distributed cache and then loaded by the map phase.
+ **Reduce-Side Join**: in this case the map phase tags each record such that records of different inputs that have to be joined will have the same tag. Each reducer will receive a tag with a list of records and perform the join.

### Jobs

+ **Distributed Cache Join**: implement word count using the distributed cache to exclude some words. The file <a href="https://github.com/michiard/CLOUDS-LAB/tree/master/mapreduce-lab/inputs/english.stop">quote</a> contains a list of words to exclude.
+ **Reduce Side Join**: implement a self-join, that is a join between two instances of the same dataset, on the twitter graph. An input file for testing purpose is <a href="https://github.com/michiard/CLOUDS-LAB/tree/master/mapreduce-lab/inputs/twitter-small">twitter-small</a> that contains lines in the form _userid_ _friendid_. The exercise is to find the two-hops friends, i.e. the friends of friends of each user.

### Instructions

+ **Distributed Cache Join**: the file is *DistributedCacheJoin.java*, for the distributed cache use the [<a href="https://hadoop.apache.org/common/docs/r0.20.2/api/org/apache/hadoop/filecache/DistributedCache.html">DistributedCache API</a>].
+ **Reduce Side Join**: the file is *ReduceSideJoin.java*. This exercise is different from the others because it does not contain any information on how to do it. The student is free to choose how to implement it.

### Example of usage
```
hadoop jar <compiled_jar> fr.eurecom.fr.mapreduce.DistributedCacheJoin 1 <big_file> <small_file> <output_path>
hadoop jar <compiled_jar> fr.eurecom.fr.mapreduce.ReduceSideJoin 1 <input_file2> <input_file1> <output_path>
```

