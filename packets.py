# Author: Miryana Hammidi (Member 1, Group 1) - packets, file splitting, END packet / SHA-256
# AI usage: written with help from Claude (Anthropic); reviewed and tested by Miryana Hammidi

import hashlib
import os
import struct

TYPE_DATA = 1
TYPE_ACK = 2
TYPE_END = 3
TYPE_NAMES = {TYPE_DATA: "DATA", TYPE_ACK: "ACK", TYPE_END: "END"}

HEADER_FORMAT = "!BIH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)   
DEFAULT_CHUNK_SIZE = 1024                      
MAX_DATA_LEN = 0xFFFF                          

def build_packet(ptype, seq, data=b""):

    if ptype not in TYPE_NAMES:
        raise ValueError(f"unknown packet type {ptype}")
    if len(data) > MAX_DATA_LEN:
        raise ValueError("data too large for the 2-byte length field")
    return struct.pack(HEADER_FORMAT, ptype, seq, len(data)) + data

def parse_packet(raw):

    if len(raw) < HEADER_SIZE:
        raise ValueError("packet shorter than header")
    ptype, seq, length = struct.unpack(HEADER_FORMAT, raw[:HEADER_SIZE])
    if ptype not in TYPE_NAMES:
        raise ValueError(f"unknown packet type {ptype}")
    data = raw[HEADER_SIZE:]
    if len(data) != length:
        raise ValueError("length field does not match payload size")
    return ptype, seq, data

class PreparedFile:

    def __init__(self, path, chunk_size=DEFAULT_CHUNK_SIZE):
        if chunk_size <= 0 or chunk_size > MAX_DATA_LEN:
            raise ValueError("chunk_size must be between 1 and 65535")
        self.path = path
        self.name = os.path.basename(path)
        self.size = os.path.getsize(path)
        self.chunk_size = chunk_size
        self.packets = []

        sha = hashlib.sha256()
        seq = 0

        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:                 
                    break
                sha.update(chunk)
                self.packets.append(build_packet(TYPE_DATA, seq, chunk))
                seq += 1

        self.num_data_packets = seq
        self.file_hash = sha.digest()         
        self.packets.append(build_packet(TYPE_END, seq, self.file_hash))
        self.total_packets = seq + 1


def format_settings(prepared, window_size, loss, reorder):
    return (
        "=" * 60 + "\n"
        f" File            : {prepared.name}\n"
        f" Size            : {prepared.size} bytes\n"
        f" Chunk size      : {prepared.chunk_size} bytes\n"
        f" Packets         : {prepared.num_data_packets} DATA + 1 END "
        f"= {prepared.total_packets}\n"
        f" Window size N   : {window_size}\n"
        f" Loss prob.      : {loss}\n"
        f" Reorder prob.   : {reorder}\n"
        f" SHA-256         : {prepared.file_hash.hex()}\n"
        + "=" * 60
    )
