Ex 01 :: Word Count
-------------------

1. Do you think the number of reducers affects word count? Try with different values and substantiate your answer

    Yes, we think so. The main reason is that several reducers can work in parallel on different machines and on smaller quantities of data in order to speed up the computation.
    Even if the reducers are run on the same machine, multi-core parallelism would enhance the computation time, too.

    Although, the executions of the first word counter on the mumak.log file provided in the exercises with 1 reducers and 3 reducers do not present any sensible difference, as we can see from the values below

    ```
    Job                                  Map    Reducer      Total

    1-Reducer CPU time spent (ms)    449,690    118,140    567,830    Total time spent by all reduces in occupied slots (ms) 117,357
    3-Reducer CPU time spent (ms)    443,950    137,660    581,610    Total time spent by all reduces in occupied slots (ms) 154,024
    ```

    The 3-reducer job was executed during the lab, so it might be slower respect to the 1-reducer one executed after the lab.


2. Use the JobTracker web interface to examine the job counters: can you explain the differences among all the implementations? For example, look at the amount of bytes shuffled by Hadoop.


    The In-Memory Combiner and the Combiner implementations are optimization of the bare word count. They aim to a reduction of the number of keys traveling the network in order to reach the reducers.

    ```
    Implementation    Byte Shuffled     CPU Time Spent Reducers(ms)     FILE: bytes written - Map
    Original          2,238,549,411                         137,660                 5,388,607,969
    In-Memory            49,753,437                          19,970                    53,671,577
    Combiner             63,530,944                          22,000                   245,285,373
    ```

    The In-Mapper implementation greatly decreases the number of Byte Shuffled in the reduce phase, thanks to the emission of way less key-value pairs and also fairly decreases the amount of CPU Time spent by the reducers.
    It also decreases the number of bytes written to disk by the mappers, of course, with great saving of I/O time both in the map phase (writing) and in the Reduce phase (reading).

    For the Combiner case, the bytes read by the reducer are less than the normal one too, as expected.
    The difference between the In-Memory combiner version and the Combiner one is that the latter is not executed at the end of a mapper, but several time during the map task, depending on the memory buffer usage
    (it can not even be called) (http://wiki.apache.org/hadoop/HadoopMapReduce). 
    In fact, the number of byte shuffled by the Combiner is a little bit more than the one produced by the In-Memory Combiner.
    So, network traffic is actually reduced, but efficiency is lower than the In-Memory case.

3. Some words of the dictionary are more used than others and reducers associated with those words will work more than the others. Do you think this is good? Why?
    
    It is not good, obviously.
    Reducers in charge of high-frequent keys are exposed to greater risks of going out of memory and to longer completion times (the completion time of the job corresponds to the completion time of the last reducer).

Ex 02 :: Stripes
----------------
1. Do you think the reducer of stripes can be used as combiner?
    
    Yes, of course. The stripes reducer simply sums up all the value contained in the stripes related to a certain word and the sum is both commutative and associative.
    In addition, removing the combiner would not change the algorithm (as it should be).

2. Do you think Stripes could be used with the in-memory combiner pattern?
  
    It is possible, but we have to be really careful, because of scalability issues.
    For large files, having many Stripes in memory may lead to memory exhaustion, because each of them store an hash map, that is a complex structure.
    Even a single Stripe too large to fit in a memory page could lead a loss of performance due to memory paging.

3. How does the number of reducer influence the behavior of the Stripes approach?
    
    The fact that having more reducers would enhance the execution parallelism and consequently the execution time is true in general.
    Doubling the number of reducers from 3 to 6, we obtained a completion time shorter than about 25% (2m00s instead of 2m32s) and a cut off of the average reduce time from 41s to 19s (50%).
    
    NOTE: statistics gathred without combiners

Ex 03 :: Pair
--------------
1. How does the number of reducer influence the behavior of the Pairs approach?
    
    According to above-mentioned, doubling the number of reducers from 2 to 4 leads to an average reduce time enhancement of 3s (from 12s to 8s), but to a completion time reduced by only 3s (2m08s against 2m11s). 
    
    NOTE: the statistics are gathered WITH combiners

2. Compared to the Stripes exercise, for a given number of reducer, how many bytes are shuffled?

    The Stripes approach generates shorter intermediate keys, respect to the Pairs approach ones. In fact, the keys are composed by only one string, while in the pair case they are composed always by two of them. 
    The preliminary level of aggregation on the line represented by the Stripes may keep low the number of key-value pairs, with respect to the Pair pattern case.

    On the other hand, the Stripes need an additional integer to be serialized on disk (the number of entries in the hash map).

    To conlude, using a combiner, might make the gap between the two approaches really narrow, because an aggregation  will be done also in the Pair approach.

    Our executions on the mumak.log file shows that the Stripe approach has generated slightly more bytes (circa 12%) than the Pair approach and it may be justified by the above-mentioned additional integer. 

Ex 04 :: Order Inversion
------------------------
1. Do you think the Order Inversion approach is 'faster' than a naive approach with multiple jobs? For example, consider implementing a compound job in which you compute the numerator and the denominator separately, and then perform the computation of the relative frequency
    
    Yes, of course. A naive approach with multiple jobs such the one proposed would have to read three times data from hdfs:
    the input file would be read twice (one to compute the denominator, one to compute the numerator) and the outputs from the first two jobs would be read by the last job, in order to compute the relative frequencies.

2. What is the impact of the use of a 'special' compound key on the amounts of shuffled bytes?
    
    Respect to simply produce a word and his occurrence, the impact of the compound ASTERISK keys consists in a null string written besides each word by the mappers.
    The serialization overhead for each word would consequently be a string of length 1 and a number (1) representing the length of the string.   
    
Ex 05 :: Joins
--------------

### Sub 01 :: Distributed Cache Join
We simply implemented the distributed cache join using the DistributedCache class. The cached file is the file containing the list of wordsbe excluded from the word count.
    
### Sub 02 :: Reduce Side Join
We implemented a reduce side join using as intermediate key the TextPair\<Join Attribute, Table Tag\> and a custom partitioner considering only the Join Attribute field of the key.
Having to output all the combinations between the left table values and the right table values (in the case the join attribute coincides, of course), this approach seemed to us way better than the one described on the slides (key = Join Attribute, value = TextPair\<Table Value\>).
    
In the way we implemented the join, the keys shuffled to the reducer are sorted by the \<Join Attribute, Table Tag\>. So, for each Join Attribute, the key-value pairs from the left table come before the ones from the right table. Thanks to this, we are able to store all the left values in a list in memory and to join them with the right values read one by one afterwards. For the sake of optimization, the Iterable object used by Hadoop to store the reducer input values does not allow to perform more than one iteration (and nested loops as well), therefore storing the values in memory is compulsory to fulfill our goals. Compared with the slides solution, our implementation need to store only half of the values (the left ones). 
