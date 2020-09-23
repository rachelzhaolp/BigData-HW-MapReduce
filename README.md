# BigData-HW-MapReduce
This repository contains solutions for four MapReduce exercises.
1. Word Count
2. Standard Deviation Computation
3. `Select` from database
4. Compute aggregations from database

<!-- tocstop -->
## Directory structure 

```
├── README.md                         <- You are here
├── Word Count
│   ├── exercise1.java                      <- java source code file
│   ├── exercise1.txt                       <- Output of the MapReduce Job:
│   ├── Problem_Statement.md                <- Problem Statement
├── Standard Deviation Computation
│   ├── exercise2.java                      <- java source code file
│   ├── exercise2.txt                       <- Output of the MapReduce Job:
│   ├── Problem_Statement.md                <- Problem Statement
├── `Select` from database
│   ├── exercise3.java                      <- java source code file
│   ├── exercise3.txt                       <- Output of the MapReduce Job:
│   ├── Problem_Statement.md                <- Problem Statement
├── Compute aggregations from database
│   ├── exercise4.java                      <- java source code file
│   ├── exercise4.txt                       <- Output of the MapReduce Job:
│   ├── Problem_Statement.md                <- Problem Statement
<!-- tocstop -->
```

## HDFS Intro:

* Create a new folder in Hadoop (not in the local filesystem)\
`hdfs dfs -mkdir <new_folder_name>`\
* List the folders and files in Hadoop\
`hdfs dfs -ls <hdfs dir>`\
eg. `hdfs dfs -ls warmup/`

* Move file from local to Hadoop\
`hdfs dfs -copyFromLocal <file_name> <hdfs dir>`\
eg.`hdfs dfs -copyFromLocal chat.txt warmup/`\

* Rename this file in Hadoop
`hdfs dfs -mv warmup/chat.txt warmup/chat_new.txt`

* Copy this file from Hadoop to the local filesystem
`hdfs dfs -copyToLocal warmup/chat_new.txt .`
note: . -> current working directory

* Extract from Hadoop a single file that is the concatenation of two files, i.e., appending one file to the other one.
(use getmerge)
  
    * Copy chat_new.txt and paste to HDFS warmup/, name the new file chat_3.txt\
    `hdfs dfs -cp warmup/chat_new.txt warmup/chat_3.txt`\
    * Merge all files in HDFS warmup and save the merged file to local(current working dir), name it chat_3.txt\
    `hdfs dfs -getmerge warmup ./chat_3.txt`\
    `ls`\
    `cat chat_3.txt`

* Move\
`hdfs dfs -mv warmup/chat_new.txt /user/mtl8764`

* Delete directory\
`hdfs dfs -rm -r -f -skipTrash warmup`

## Run MapReduce with gradle(Mac)

1. `mkdir <new_directory_name>` -> new directory for the map reduce project
2. `cd <the_new_directory_name>`
3. `gradle wrapper`
4. `rm gradle.bat`
5. `mkdir src` -> a directory to store the java code, then copy the java code into it
6. `vi build.gradle`
7. Enter I -> into the edit mode
8. Change `mainClassName` and `srcDir`
9. Exit, `:wq`, enter -> save and exit vim. (`:q!` Quit without change)
10. `./gradlew build` -> build the jar
11. `hdfs dfs -copyFromLocal <text.txt> <data/>` -> copy data into the data/ in Hadoop FileSyetem\
12. `yarn jar  <filepath of the jar> <hdfs path of the data> <hdfs path of the output>`
eg: `yarn jar <./build/libs/mapreduce.jar> <data/text.txt> <lab1example/wc/output>`\
Notice: The output directory should not exist at the beginning
13. Merge the output: \
`hdfs dfs -getmerge lab1example/wc/output ./wc_output.txt`\
14. DO NOT USE cat TO VIEW THE RESULT; USE vi
