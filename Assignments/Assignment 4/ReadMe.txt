Command to execute the MapReduce Job:
sudo -u <username> <path_of_hadoop> jar equijoin.jar Equijoin <HDFSinputFile> <HDFSoutputFile>


Driver (Equijoin):
The configuration of the MapReduce job is defined in the Driver function (main). 
The following information are defined:
1. Name for the job.
2. Command line arguments for the job.
3. Java class that performs the Map tasks.
4. Java class that performs the Reduce tasks
5. Data type of the keys in Mapper task's output.
6. Data type of the values in Mapper task's output.
7. Data type of the keys in Reducer task's output.
8. Data type of the values in Reducer task's output.
9. Input and Output files for the MapReduce task.


Mapper (EquijoinMapper):
This function reads the input file from HDFS and generate a list of key-value pairs.
For each line in the input file, a key-value pair is generated. 
The join column is used as the key and the entire line is it's corresponding value.


Reducer (EquijoinReducer):
This function receives input of the form <key, list(values)> where the actual equijoin operation is performed.
First, we seperate the lines present in the "list(values)" into 2 lists based on the table name.
Next, we generate all possible non-repeating pairs from the two lists (similar to nested loop joins). This gives us the required result.