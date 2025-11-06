Using my database and showing existing tables

![img.png](imgs/img.png)

Creating new table inside the database

![img_1.png](imgs/img_1.png)

At this point this is the content of the hdfs local and remote (there is nothing in the database dir yet)

![img_2.png](imgs/img_2.png)

Lets check the table metadata / using formatted option

![img_3.png](imgs/img_3.png)

the important line is this one:
Location: | hdfs://hadoop-namenode:9000/user/andb34/database

Also lets try the extended option in describe command

![img_4.png](imgs/img_4.png)

Copy the files from hadoop client to the database

![img_5.png](imgs/img_5.png)

Lets check the content with the simple query

![img_6.png](imgs/img_6.png)

Finally, drop the table and check the remote hdfs

![img_7.png](imgs/img_7.png)

![img_8.png](imgs/img_8.png)

