Question 1

The default block size on HDFS is 128 MB. The default replication factor of HDFS on Dataproc is 2.



Question 2

The completion time of this task is 5 min 22 sec.



Question 3

The completion time of this task is 3 min 25 sec. The performance is getting better in terms of completion time compared with Q2. This is because of the parallelism.



Question 4

The completion time of this task is 3 min 21 sec. The performance is getting slightly better in terms of completion time compared with Q3. It might be because a larger block size (128 MB) would decrease parallelism.



Question 5

The completion time of this task without killing one of the worker is 47 min 18 sec. Killing one of the worker after submitting the task doesn't cause the termination of the task. But it does slow down the speed of completing the task. The completion time is 1 hr 37 min.



Question 6

The completion time of this task is 46 min 6 sec, slightly shorter than Q5. This might be because the default value of replication factor is 2 which is used for Q5, but in this task  we set the replication factor to be 1.



Question 7

The completion time of this task is 45 min 22 sec. The performance is getting slightly better in terms of completion time compared with Q5. It might be because a larger block size (128 MB) would decrease parallelism. The cluster would be underutilized because of large block size. There would be fewer splits and in turn would be fewer map tasks, which slowing down the job.



Question 8

The completion time of this task is 1 hr 24 min.


Question 9
There are 59355124 articles in the database have a rank greater than 0.5.


Question 10
Yes, it is more feasible and efficient. As once connection is acceptiong, it will sending data continuously, and we can select ranks using an almost real-time stream processing. While in Part 2 Task 1, we can only generate filter file, and then count the number.


Question 11
I spend 3 days to finish this assignment.

