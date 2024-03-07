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
output_run_num = 0

# pass 0
# Initialize buffer
buf = [None] * num_of_buf

# read in file as binary
f = open(input_file_name, "rb")

if os.path.exists("./temp"):
    shutil.rmtree("./temp")
os.makedirs("./temp")

for output_run_num in range(num_of_runs):
    # write out one sorted run
    for i in range(num_of_buf):
        buf[i] = f.read(record_size)
    buf.sort()
    sorted_data = b''.join(buf)
    out = open(f"./temp/pass{pass_num}_{output_run_num}.dat", "ab")
    out.write(sorted_data)
    out.close()
    # reset buffer for the next run
    buf = [None] * num_of_buf
f.close()

num_of_input_buf = num_of_buf - 1
num_of_output_buf = 1

# remaining passes
total_num_of_passes = math.ceil(math.log(num_of_runs, num_of_input_buf))

prev_max_run_num = output_run_num
load_run_start = 0



# merge
# passes
while pass_num != total_num_of_passes:
    # runs
    pass_num += 1
    next_input_run_to_load = 0
    output_run_num = 0 
    remain_runs_to_load = prev_max_run_num + 1
    
    print(remain_runs_to_load)


    # runs
    while remain_runs_to_load > 0:
        input_buf = []
        output_file = open(f"./temp/pass{pass_num}_{output_run_num}.dat", "ab")

        for i in range(min(num_of_input_buf, remain_runs_to_load)):
            # print(num_of_input_buf, prev_max_run_num)
            input_buf.append(open(f"./temp/pass{pass_num-1}_{next_input_run_to_load}.dat", "rb"))
            next_input_run_to_load += 1
            remain_runs_to_load -= 1
            # print(next_run_to_load)

        heap = []
        for file in input_buf:
            if file != None:
                record = file.read(record_size)
                if record:
                    heapq.heappush(heap, (record, file))
        while heap:
            record, file = heapq.heappop(heap)
            output_file.write(record)
            next_record = file.read(record_size)
            if next_record:
                heapq.heappush(heap, (next_record, file))
        input_buf.clear()
        output_run_num += 1
    
    prev_max_run_num = output_run_num - 1 # TODO: fix this -1
    print(f"output_run_num is {output_run_num}")