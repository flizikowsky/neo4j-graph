Creating the table and showing the created table.

![img.png](imgs/img.png)

File employee.txt examination: Values in the column named depart_title are stored in arrays in string type. 
Values are separated by, a typical for nested collection structures, the control character (end of transmission)
Here: https://www.ascii-code.com/characters/control-characters, the character is labeled as the Code 4.
Values are separated by the pipe character '|'.

Here is the peek for the data in employee.txt:

![img_1.png](imgs/img_1.png)

Lets now load the data into the table:

![img_2.png](imgs/img_2.png)

And see if the data is loaded:

![img_3.png](imgs/img_3.png)

Now, we will run some queries:

Query a:

![img_4.png](imgs/img_4.png)

Query b:

![img_5.png](imgs/img_5.png)

Query c:

![img_6.png](imgs/img_6.png)

Query d:

![img_7.png](imgs/img_7.png)

Query e:

![img_8.png](imgs/img_8.png)

Query f:

![img_9.png](imgs/img_9.png)

Query g:

![img_10.png](imgs/img_10.png)

Query h:

![img_11.png](imgs/img_11.png)

Query i:

![img_12.png](imgs/img_12.png)

Query j:

![img_13.png](imgs/img_13.png)

Load data into hadoop from local

![img_14.png](imgs/img_14.png)

Tables were created in Hive.

![img_15.png](imgs/img_15.png)

Load data into sales

![img_16.png](imgs/img_16.png)

Load data into companies

![img_17.png](imgs/img_17.png)

Running select statement with JOIN clause

![img_18.png](imgs/img_18.png)

This query is showing date, company, city and value from joined tables - sales and comapnies.
Join is invokin a hive job which can be seen in the logs. It is processing a local map reduce job - join.

Creating companies2 table

![img_19.png](imgs/img_19.png)

Load data into companies2 and see if it is loaded

![img_20.png](imgs/img_20.png)

Running select statement with JOIN clause

![img_21.png](imgs/img_21.png)

This query is also showing date, company, value and in this case the owner for every sale.
This is also the same case as in the previous query - map reduce job is running.

