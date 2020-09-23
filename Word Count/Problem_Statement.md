
# Problem 1
The data for this problem comes from Google’s project for counting word frequencies in its entire Google Books collection. You are given two files:
1. one file reports 1 grams (single words) 
    * Format: word \\s+ year \\s+ number of occurrences \\s+ number of volumes \\s+ …\
    the regex “\\s+” will match any kind of whitespace (space, tab etc)

2. one file reports 2 grams (pairs of words; one following each other in text). 
    * Format: word \\s+ word \\s+ year \\s+ number of occurrences \\s+ number of volumes \\s+...
    the regex “\\s+” will match any kind of whitespace (space, tab etc)

The data represents the number of occurrences of a particular word (or pairs of words) in a given year across all books available by Google Books. The number of volumes/books containing a word (or pairs of words) is also reported.

Write a MapReduce program that reports the `average number of volumes per year for words containing
the following three substrings: ‘nu,’ ‘chi,’ ‘haw’`. The final output should show the year, substring, and average number of volumes (across both bigram and unigram formats) where the substring appears in that year. 

For example:\
2000,nu,345\
2010,nu,200\
1998,chi,31

Note:
1. The ‘year’ column may include erroneous values which can be a string. If the year field is a string, the
record should be discarded.
2. If each word in the bi-gram includes the string, it should be counted twice in the average. For example,
for the bi-gram “nugi hinunu” with volume of 10, when calculating the average, its contribution to the
numerator should be 2 times 10 and in the denominator it should be 2. A unigram counts only once
regardless of the number of occurrence of “nu” in the word.
3. Do this in the most efficient way with a single MapReduce job. Beyond the file formats described above,
you are not allowed to make any structural assumptions on the data; e.g., that the 2 gram file contains
more fields compared to the 1 gram file.
