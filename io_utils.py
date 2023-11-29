
import pyzstd
import io
from tqdm import tqdm
from tensorflow.io import gfile
import json
from more_itertools import chunked
from pathlib import Path
# from src.utils.logging_utils import init_logger
# logger = init_logger()


def pack_byte_arrays(*byte_arrays):
    """
    Concatenates arbitrary byte arrays with a header that indicates their sizes.

    Parameters:
    - byte_arrays (*byte): Variable number of byte array arguments.

    Returns:
    - bytes: A single byte array with a header of sizes followed by the original byte arrays.
    """

    # Create a header with the sizes of the byte arrays, each size is a 4-byte integer
    content = bytearray()
    content += len(byte_arrays).to_bytes(4, 'big')  # Convert the header size to 4 bytes
    for byte_array in byte_arrays:
        content += len(byte_array).to_bytes(4, 'big')  # Convert the size to 4 bytes
    # Concatenate the header and the byte arrays
    for byte_array in byte_arrays:
        content += byte_array

    return bytes(content)

def unpack_byte_arrays(packed_data, nbytes = 4):
    packed_array = bytearray(packed_data)
    num_arrays = int.from_bytes(packed_data[:4], 'big')
    sizes = []
    packed_array = packed_array[4:]  # Remove the first 4 bytes
    size_data = packed_array[:4*num_arrays]  # Read the size data
    array_data = packed_array[4*num_arrays:]  # Read the array data
    
    for offset in range(0,len(size_data),nbytes):
        array_size = int.from_bytes(size_data[offset:offset+nbytes], 'big')
        sizes.append(array_size)
    
    offset = 0
    array_list = []
    for i in range(num_arrays):
        data = array_data[offset:offset+sizes[i]]
        offset += sizes[i]
        array_list.append(data)
    return array_list

class MultiBytesIOWriter(io.RawIOBase):
    def __init__(self,
                 generator,
                 ):
        self.current_buffer = None
        self.generator = generator

    def readinto(self, b):
        bytes_read = 0
        
        while bytes_read < len(b):
            # Check if current_buffer is empty
            if not self.current_buffer:
                try:
                        # Get next BytesIO object from generator
                    self.current_buffer = bytearray(next(self.generator))
                except StopIteration:
                    # Edge Case 1: Generator is exhausted, indicate EOF
                    return 0 if bytes_read == 0 else bytes_read
                # Read from the current BytesIO object into current_buffer
                # Edge Case 3: Empty BytesIO Objects
                if not self.current_buffer:
                    continue
            
            # Calculate remaining space in 'b' and bytes available in current_buffer
            remaining_space = len(b) - bytes_read
            available_bytes = len(self.current_buffer)
            bytes_to_copy = min(remaining_space, available_bytes) # Calculate how many bytes to copy
            b[bytes_read:bytes_read + bytes_to_copy] = self.current_buffer[:bytes_to_copy]
            self.current_buffer = self.current_buffer[bytes_to_copy:]
            bytes_read += bytes_to_copy
        return bytes_read

class MultiBytesIOReader:
    def __init__(self, decompressedStream: io.BytesIO,
                 buffer_size=65536,
                 file_position=0,
                 ):
        self.decompressedStream = decompressedStream
        self.buffer_size = buffer_size
        self.incomplete_line = bytearray()
        self.position = file_position
    
    def seek(self, position):
        self.position = position

    def tell(self):
        return self.position
    
    def __iter__(self):
        self.decompressedStream.seek(self.position)
        while True:
            buffer = self.decompressedStream.read(self.buffer_size)
            if not buffer:
                break
            buffer = self.incomplete_line + buffer
            self.incomplete_line = bytearray()
            lines = buffer.split(b'\n')
            if lines and lines[-1]:
                self.incomplete_line = lines.pop()
            for line in lines:
                if line:
                    self.position += len(line)+1
                    yield line
        if self.incomplete_line:
            self.position += len(self.incomplete_line)
            yield self.incomplete_line


import time

# class CallbackWriter:
#     """
#     Description: A class that writes to a file descriptor and logs the progress once every `frequency` seconds
#     """
#     def __init__(self, fd, filename, frequency=300):
#         # self.logger = logger
#         self.fd = fd
#         self.filename = filename
#         self.frequency = frequency # in seconds
#         self.last_run = 0
        
#     def __call__(self, total_input, total_output, _, write_data):
#         current_time = time.time()
#         data = bytes(write_data)
#         self.fd.write(data)
#         if current_time - self.last_run > self.frequency:
#             self.last_run = current_time
#             print(f"timestamp={current_time} out_mb={total_input/(1024**2):.2f} out_c_mb={total_output/(1024**2):.2f} filename={self.filename}")
#             self.fd.flush()
#         self.total_input = total_input
#         self.total_output = total_output

def parse_path(path_to_model):
    path_to_model = str(Path(str(path_to_model)))
    if path_to_model.startswith("gs:/"):
        if not path_to_model.startswith("gs://"):
            path_to_model = path_to_model.replace("gs:/", "gs://")
    return path_to_model

class GLOBAL_OBJECT:
    total_output=0
    
def write_to_file(itr, output_path, batch_size=1,**kwargs):
    obj = GLOBAL_OBJECT()
    output_path = parse_path(output_path)
    itr = iter(itr)
    if batch_size>1:
        itr = chunked(itr, batch_size)
        itr = iter(map(lambda x: b'\n'.join(x)+b'\n', itr))

    stream = MultiBytesIOWriter(itr,**kwargs)
    with tqdm() as pbar:
        with gfile.GFile(output_path, 'wb') as f:
            def callback(total_input, total_output, _, write_data):
                data = bytes(write_data)
                desc = dict(out_mb=f"{total_input/(1024**2):.2f}", out_c_mb=f"{total_output/(1024**2):.2f}")
                pbar.set_description(repr(desc))
                pbar.update(len(data))
                f.write(data)
                obj.total_output = total_output
            pyzstd.compress_stream(stream, None, callback=callback)
    msg = f"Done writing into {output_path} a total of {obj.total_output/(1024**2):.2f}MB"
    print(msg)


def serialize(data_point):
    return json.dumps(data_point).encode('utf-8')


def deserialize(data_point):
    return json.loads(data_point.decode('utf-8'))
