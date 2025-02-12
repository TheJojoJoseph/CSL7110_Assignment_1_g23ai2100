Input Pairs for the Reduce Phase:
The input to the Reduce phase consists of key-value pairs where:

The key is a unique word from the input text.
The value is a list (or iterable) of integers representing the frequency count from the Map phase.
For the given example:

"lucky to get up to get up to up up"
The word counts from the Map phase will be:

("lucky", [1])
("to", [1, 1, 1])
("get", [1, 1])
("up", [1, 1, 1, 1])
These pairs are grouped and sent to the Reduce phase.

Types of Keys and Values in the Reduce Phase:
Input to Reduce phase:

Key: Text (word)
Value: Iterable<IntWritable> (list of integers representing word occurrences)
Output from Reduce phase:

Key: Text (word)
Value: IntWritable (final count of occurrences)
For example, the Reduce phase processes:


("up", [1, 1, 1, 1]) → ("up", 4)
("to", [1, 1, 1]) → ("to", 3)
("get", [1, 1]) → ("get", 2)
("lucky", [1]) → ("lucky", 1)
This follows the standard word count pattern in MapReduce, where:

The Mapper emits (word, 1).
The Reducer sums up the counts and emits (word, total_count).
