import sys
import os
import math

# parse command arguments
program_name, input_file_name, output_file_name, record_size, key_size, amt_of_mem, is_ascending = sys.argv

input_file_name = sys.argv[1]
output_file_name = sys.argv[2]
record_size = int(sys.argv[3])
key_size = int(sys.argv[4])
amt_of_mem = int(sys.argv[5])
is_ascending = False if sys.argv == "1" else True

# check input file size
input_size = os.path.getsize(input_file_name)
num_of_records = input_size // record_size
buf_size = math.floor(amt_of_mem / record_size)
num_of_runs = math.ceil(num_of_records / buf_size)
run_size = buf_size
# print(buf_size)
# print(num_of_runs)

# read in file as binary

# pass 0
# Initialize buffer
buf = []
f = open(input_file_name, "rb")
for j in range(num_of_runs):
    for i in range(buf_size):
        buf.append(f.read(record_size))
    buf.sort()
    sorted_data = b''.join(buf)

    out = open(output_file_name, "ab")
    # print(sorted_data)

    out.write(sorted_data)
    buf.clear()

# remaining passes
num_of_merge_passes = math.log(num_of_runs, buf_size - 1)


# for i in range(num_of_merge_passes):