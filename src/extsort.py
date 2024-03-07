import sys
import os
import math
import shutil
import heapq

# parse command arguments
program_name, input_file_name, output_file_name, record_size, key_size, amt_of_mem, is_ascending = sys.argv

input_file_name = sys.argv[1]
output_file_name = sys.argv[2]
record_size = int(sys.argv[3]) # in B
key_size = int(sys.argv[4]) # in B
amt_of_mem = float(sys.argv[5]) # in MB

num_of_buf = math.floor(amt_of_mem * 1024 * 1024 / record_size)
is_ascending = False if sys.argv == "1" else True

# check input file size
input_file_size = os.path.getsize(input_file_name)
num_of_records = math.floor(input_file_size / record_size)
num_of_runs = math.ceil(num_of_records / num_of_buf)

output_file_number = 0

pass_num = 0
run_num = 0

# pass 0
# Initialize buffer
buf = [None] * num_of_buf

# read in file as binary
f = open(input_file_name, "rb")

if os.path.exists("./temp"):
    shutil.rmtree("./temp")
os.makedirs("./temp")

for run_num in range(num_of_runs):
    # write out one sorted run
    for i in range(num_of_buf):
        buf[i] = f.read(record_size)
    buf.sort()
    # print(min(buf))
    sorted_data = b''.join(buf)
    # print(sorted_data) # TODO: where did  went?
    out = open(f"./temp/pass{pass_num}_{run_num}.dat", "ab")
    out.write(sorted_data)
    out.close()
    # reset buffer for the next run
    buf = [None] * num_of_buf
f.close()

# remaining passes
pass_num += 1
run_num = 0
num_of_input_buf = num_of_buf - 1
num_of_output_buf = 1



# 先写个merge一次的
buf = [None] * num_of_input_buf
input_file_buf = [None] * num_of_input_buf

output_file_buf = open(f"./temp/pass{pass_num}_run{run_num}.dat", "ab")

for i in range(num_of_input_buf):
    input_file_buf[i] = open(f"./temp/pass{pass_num-1}_{i}.dat", "rb")

heap = []
for file in input_file_buf:
    record = file.read(record_size)
    if record:
        heapq.heappush(heap, (record, file))

while heap:
    record, file = heapq.heappop(heap)
    output_file_buf.write(record)
    next_record = file.read(record_size)
    if next_record:
        heapq.heappush(heap, (next_record, file))



# # store file pointers and load record to buffer
# for i in range(num_of_input_buf):
#     # TODO: last file might have less elements, when to stop?
#     f = open(f"./temp/pass{pass_num-1}_{i}.dat", "rb")
#     input_file_buf[i] = f
#     buf[i] = input_file_buf[i].read(record_size)


# size_of_run = num_of_buf * num_of_input_buf
# for i in range(size_of_run):
#     min_record = min(buf)
#     min_index = buf.index(min_record)
#     output_file_buf.write(min_record)
#     # TODO: what happen if it read to the end of file? It just read b''
#     buf[min_index] = input_file_buf[min_index].read(record_size)
# print(buf)