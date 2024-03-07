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

# initialize variables
num_of_buf = math.floor(amt_of_mem * 1024 * 1024 / record_size)
is_ascending = False if sys.argv == "1" else True # TODO: fix ascending

# check input file size
input_file_size = os.path.getsize(input_file_name)
num_of_records = math.floor(input_file_size / record_size)
num_of_runs = math.ceil(num_of_records / num_of_buf)

if os.path.exists("./temp"):
    shutil.rmtree("./temp")
os.makedirs("./temp")

pass_num = 0

# pass 0
# Initialize buffer
buf = [None] * num_of_buf

# read in file as binary
with open(input_file_name, "rb") as input_file:
    for cur_output_run_num in range(num_of_runs):
        # write out one sorted run
        for i in range(num_of_buf):
            buf[i] = input_file.read(record_size)
        buf.sort(reverse=is_ascending)
        with open(f"./temp/pass{pass_num}_{cur_output_run_num}.dat", "ab") as output_file:
            for record in buf:
                output_file.write(record)
# pass 0 done
input_buf_size = num_of_buf - 1

# remaining passes
total_num_of_passes = math.ceil(math.log(num_of_runs, input_buf_size))

prev_max_run_num = cur_output_run_num # 12

# merge
# passes
while pass_num != total_num_of_passes:
    # runs
    pass_num += 1
    next_input_run_to_load = 0
    cur_output_run_num = -1 # current output run number
    remain_runs_to_load = prev_max_run_num + 1
    
    # runs
    while remain_runs_to_load > 0: # 13
        cur_output_run_num += 1
        input_buf = []
        if pass_num == total_num_of_passes:
            output_file = open(f"./{output_file_name}", "ab")
        else:
            output_file = open(f"./temp/pass{pass_num}_{cur_output_run_num}.dat", "ab")

        for i in range(min(input_buf_size, remain_runs_to_load)):
            input_buf.append(open(f"./temp/pass{pass_num-1}_{next_input_run_to_load}.dat", "rb"))
            next_input_run_to_load += 1 # loaded runs, so update
            remain_runs_to_load -= 1

        # merge
        heap = []
        for file in input_buf:
            record = file.read(record_size)
            if record:
                heapq.heappush(heap, (record, file))
        while heap:
            record, file = heapq.heappop(heap)
            output_file.write(record)
            next_record = file.read(record_size)
            if next_record:
                heapq.heappush(heap, (next_record, file))
        for file in input_buf:
            file.close()
        input_buf.clear()
    
    prev_max_run_num = cur_output_run_num

    output_file.close()
    # delete old files
    for file_name in os.listdir("./temp"):
        if file_name.startswith(f"pass{pass_num - 1}"):
            os.remove(os.path.join("./temp", file_name))

# delete temp folder
shutil.rmtree("./temp")
    