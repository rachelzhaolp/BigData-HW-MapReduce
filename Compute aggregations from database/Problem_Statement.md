# Problem 4

The data set is the same as in the Problem 3. 

For each artist in the data set, compute the `maximum duration` across all of their songs. 

The output should be: artist, max duration.

In addition, the management of your firm wants the artists’ names to be `sorted` across all files based on
the first character. This means that the each output file of your MapReduce job must be sorted by the
first character of an artist’s name. You cannot take the output files and then sort them in a spreadsheet
software. You are only allowed to concatenate the output files in the end.

In order to save computing resources for your firm, you have to use 5 reducers.