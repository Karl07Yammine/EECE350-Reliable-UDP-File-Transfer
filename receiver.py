# Author: Karl Yammine (Member 3, Group 1) - receiver and acknowledgements
# Drafted with help from Claude (Anthropic), reviewed and tested by Karl Yammine

import argparse
import hashlib
import socket
import sys
import time

from packets import build_packet, parse_packet, TYPE_ACK, TYPE_END, TYPE_NAMES
MAX_UDP_PAYLOAD = 65535


class C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    RED = "\033[91m"
    GREY = "\033[90m"

    @classmethod
    def disable(cls):
        for name in ("RESET", "BOLD", "GREEN", "YELLOW", "CYAN", "MAGENTA", "RED", "GREY"):
            setattr(cls, name, "")


def safe_parse(raw):
    try:
        return parse_packet(raw)
    except ValueError:
        return None


class Receiver:
    def __init__(self, host, port, out_path, idle_timeout=30.0, linger=3.0, on_event=None):
        self.host = host
        self.port = port
        self.out_path = out_path
        self.idle_timeout = idle_timeout
        self.linger = linger
        self.on_event = on_event

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))

        self.expected = 0
        self.buffer = {}
        self.chunks = []
        self.end_hash = None
        self.end_seq = None
        self.done = False
        self.sender_addr = None

        self.stats = {
            "received": 0, "in_order": 0, "out_of_order": 0, "gap_fills": 0,
            "duplicates": 0, "malformed": 0, "acks_sent": 0, "dup_acks_sent": 0,
        }
        self.start_time = None
        self.end_time = None

    def log(self, text, colour=""):
        stamp = time.strftime("%H:%M:%S")
        ms = int((time.time() % 1) * 1000)
        print(f"{C.GREY}[{stamp}.{ms:03d}]{C.RESET} {colour}{text}{C.RESET}", flush=True)
        if self.on_event:
            self.on_event(text)

    def show_settings(self):
        print(f"{C.BOLD}{'=' * 60}")
        print("  RECEIVER  (Member 3 - Karl Yammine)")
        print(f"{'=' * 60}{C.RESET}")
        print(f"  Listening on   : {self.host}:{self.port}")
        print(f"  Output file    : {self.out_path}")
        print(f"  Idle timeout   : {self.idle_timeout}s   Linger after END: {self.linger}s")
        print(f"{C.BOLD}{'-' * 60}{C.RESET}")
        print("  Waiting for sender...\n", flush=True)

    def send_ack(self, duplicate=False):
        self.sock.sendto(build_packet(TYPE_ACK, self.expected), self.sender_addr)
        self.stats["acks_sent"] += 1
        if duplicate:
            self.stats["dup_acks_sent"] += 1

    def deliver(self, ptype, seq, payload):
        if ptype == TYPE_END:
            self.end_hash = payload.hex()
            self.end_seq = seq
            self.done = True
        else:
            self.chunks.append(payload)
        self.expected = seq + 1

    def flush_buffer(self):
        flushed = 0
        while self.expected in self.buffer and not self.done:
            ptype, payload = self.buffer.pop(self.expected)
            self.deliver(ptype, self.expected, payload)
            flushed += 1
        return flushed

    def handle_packet(self, ptype, seq, payload):
        name = TYPE_NAMES.get(ptype, f"TYPE{ptype}")
        self.stats["received"] += 1

        if seq < self.expected or seq in self.buffer:
            self.stats["duplicates"] += 1
            self.send_ack(duplicate=True)
            where = "already delivered" if seq < self.expected else "already buffered"
            self.log(f"RECV {name} {seq} duplicate ({where}), discarded "
                     f"→ RE-ACK {self.expected}", C.MAGENTA)
            return

        if seq == self.expected:
            had_gap = bool(self.buffer)
            self.deliver(ptype, seq, payload)
            flushed = self.flush_buffer()
            self.send_ack()
            self.stats["in_order"] += 1
            if had_gap and flushed:
                self.stats["gap_fills"] += 1
                self.log(f"RECV {name} {seq} fills gap (+{flushed} buffered delivered) "
                         f"→ ACK {self.expected}", C.CYAN)
            else:
                self.log(f"RECV {name} {seq} in order → ACK {self.expected}", C.GREEN)
            return

        self.buffer[seq] = (ptype, payload)
        self.stats["out_of_order"] += 1
        self.send_ack(duplicate=True)
        self.log(f"RECV {name} {seq} out of order, buffered → DUP ACK {self.expected}"
                 f"   {C.GREY}(buffer: {sorted(self.buffer)}){C.RESET}", C.YELLOW)

    def run(self):
        self.show_settings()
        self.sock.settimeout(self.idle_timeout)
        try:
            while not self.done:
                try:
                    raw, addr = self.sock.recvfrom(MAX_UDP_PAYLOAD)
                except socket.timeout:
                    self.log(f"No packet for {self.idle_timeout}s, giving up.", C.RED)
                    return False
                if self.start_time is None:
                    self.start_time = time.time()
                    self.log(f"First packet from {addr[0]}:{addr[1]}", C.BOLD)
                self.sender_addr = addr

                parsed = safe_parse(raw)
                if parsed is None:
                    self.stats["malformed"] += 1
                    self.log(f"Malformed datagram ({len(raw)} bytes), ignored", C.RED)
                    continue
                ptype, seq, payload = parsed
                if ptype == TYPE_ACK:
                    continue
                self.handle_packet(ptype, seq, payload)

            self.end_time = time.time()
            self.log(f"END {self.end_seq} received in order → ACK {self.expected}, "
                     f"transfer complete", C.BOLD + C.GREEN)

            self.linger_phase()
            match = self.write_and_verify()
            self.show_summary(match)
            return match
        except KeyboardInterrupt:
            self.log("Interrupted by user.", C.RED)
            return False
        finally:
            self.sock.close()

    def linger_phase(self):
        if self.linger <= 0:
            return
        self.log(f"Staying alive {self.linger}s to re-ACK any retransmitted END...", C.GREY)
        deadline = time.time() + self.linger
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            self.sock.settimeout(remaining)
            try:
                raw, addr = self.sock.recvfrom(MAX_UDP_PAYLOAD)
            except socket.timeout:
                break
            parsed = safe_parse(raw)
            if parsed is None or parsed[0] == TYPE_ACK:
                continue
            self.sender_addr = addr
            self.stats["received"] += 1
            self.stats["duplicates"] += 1
            self.send_ack(duplicate=True)
            self.log(f"RECV {TYPE_NAMES.get(parsed[0], '?')} {parsed[1]} after completion, "
                     f"discarded → RE-ACK {self.expected}", C.MAGENTA)

    def write_and_verify(self):
        data = b"".join(self.chunks)
        with open(self.out_path, "wb") as f:
            f.write(data)
        local_hash = hashlib.sha256(data).hexdigest()
        self.log(f"File written: {self.out_path} ({len(data)} bytes, "
                 f"{len(self.chunks)} data packets)")
        self.log(f"SHA-256 received : {self.end_hash}")
        self.log(f"SHA-256 computed : {local_hash}")
        match = local_hash == self.end_hash
        if match:
            self.log("HASH CHECK: MATCH ✔", C.BOLD + C.GREEN)
        else:
            self.log("HASH CHECK: MISMATCH ✘", C.BOLD + C.RED)
        return match

    def show_summary(self, match):
        s = self.stats
        elapsed = (self.end_time or time.time()) - (self.start_time or time.time())
        size = sum(len(c) for c in self.chunks)
        rate = size / elapsed / 1024 if elapsed > 0 else 0
        verdict = f"{C.GREEN}MATCH{C.RESET}" if match else f"{C.RED}MISMATCH{C.RESET}"
        print(f"\n{C.BOLD}{'=' * 60}")
        print("  RECEIVER SUMMARY")
        print(f"{'=' * 60}{C.RESET}")
        print(f"  Packets received (total)     : {s['received']}")
        print(f"  Delivered in order           : {s['in_order']}")
        print(f"  Out of order (buffered)      : {s['out_of_order']}")
        print(f"  Gaps filled                  : {s['gap_fills']}")
        print(f"  Duplicates discarded         : {s['duplicates']}")
        print(f"  Malformed datagrams          : {s['malformed']}")
        print(f"  ACKs sent (total)            : {s['acks_sent']}")
        print(f"  Duplicate ACKs sent          : {s['dup_acks_sent']}")
        print(f"  Bytes written                : {size}")
        print(f"  Transfer time                : {elapsed:.3f} s  ({rate:.1f} KB/s)")
        print(f"  File integrity (SHA-256)     : {verdict}")
        print(f"{C.BOLD}{'=' * 60}{C.RESET}")


def main():
    p = argparse.ArgumentParser(description="Reliable UDP file receiver (Member 3)")
    p.add_argument("--host", default="127.0.0.1", help="address to bind (default 127.0.0.1)")
    p.add_argument("--port", type=int, default=5000, help="UDP port to listen on (default 5000)")
    p.add_argument("--out", default="received_file", help="path to write the received file")
    p.add_argument("--idle-timeout", type=float, default=30.0,
                   help="seconds with no packets before giving up (default 30)")
    p.add_argument("--linger", type=float, default=3.0,
                   help="seconds to keep re-ACKing after END (default 3)")
    p.add_argument("--no-color", action="store_true", help="disable coloured output")
    args = p.parse_args()

    if args.no_color or not sys.stdout.isatty():
        C.disable()

    r = Receiver(args.host, args.port, args.out, args.idle_timeout, args.linger)
    ok = r.run()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
