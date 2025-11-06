Creating table

![img.png](imgs/img.png)

Load data and see if its in the table

![img_1.png](imgs/img_1.png)

The column yearsofexperience was automatically assigned a value '3' since  all columns in the files which are connected to a parition column
are skipped and rows get parition assigned during the create statement. For id=104 the yearofexperience was changed from value '5' to '3'.

Creating non-paritioned table and showing it

![img_2.png](imgs/img_2.png)

Loading data into the table and checking if it loaded properly

![img_3.png](imgs/img_3.png)

Creating the second paritioned table called partitioned_test_managed_DP and checking if it was created properly

![img_4.png](imgs/img_4.png)

Changing the options of hive execution engine

![img_5.png](imgs/img_5.png)

Loading data into paritioned_test_managed_DP and checking if it was loaded properly

![img_6.png](imgs/img_6.png)

The number of partitions is 3, because values that can be observed in 'yearofexperience' are '3', '5' or NULL.
We can also see the difference if we display the partitions:

![img_7.png](imgs/img_7.png)

Using DESCRIBE function does not show any particular difference between the two tables.

Creating the table partitioned_test_managed_PC and checking if it was created properly

![img_8.png](imgs/img_8.png)

Loading data into partitioned_test_managed_PC and checking if it was loaded properly

![img_9.png](imgs/img_9.png)

Checking the metadata of the new table

![img_10.png](imgs/img_10.png)sshow

Check the partitioning and bucketing for each partition

![img_11.png](imgs/img_11.png)

![img_12.png](imgs/img_12.png)

So the partitioned cities are then divided into buckets based on yearofexperience (so the 3 files in each), they are stored in binary files

