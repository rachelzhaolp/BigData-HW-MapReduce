# Problem 3 
The data for this problem is a subset of the million song database(42GB) in csv format.

Your task is to `extract the song title (column 1), artist’s name (column 3) and duration (column 4)` for all
songs published between the years 2000 and 2010.The year is in column 166 – note that some year
entries can be erroneous, and should be discarded. Additionally, you can assume that all songs are
unique, so there is no need to remove any duplicates. You should do this as efficiently as possible in
MapReduce.