# Member 2: Ali Yassine - sender window and retransmission.
# AI usage: written with help from Claude (Anthropic); reviewed and tested by me.

import argparse
import socket
import threading
import time

from packets import PreparedFile, parse_packet, format_settings, TYPE_ACK, DEFAULT_CHUNK_SIZE
from channel import UnreliableChannel

RESET = "\033[0m"
COLORS = {
    "SEND": "\033[32m", "DROPPED": "\033[31m", "REORDERED": "\033[35m",
    "FLUSH": "\033[33m", "ACK": "\033[36m", "DUP": "\033[90m",
    "TIMEOUT": "\033[1;91m", "3": "\033[1;91m", "WINDOW": "\033[94m",
}
_print_lock = threading.Lock()


def log(message):
    color = COLORS.get(message.split(" ", 1)[0], "")
    with _print_lock:
        print(f"{color}{message}{RESET}", flush=True)


# Part 1: TCP-style timeout estimator
class RTOEstimator:
    def __init__(self, initial_rto=1.0, alpha=0.125, beta=0.25, min_rto=0.2, max_rto=3.0):
        self.alpha = alpha
        self.beta = beta
        self.min_rto = min_rto
        self.max_rto = max_rto
        self.initial_rto = initial_rto
        self.srtt = None
        self.rttvar = None
        self.rto = initial_rto

    def _clamp(self, value):
        return min(self.max_rto, max(self.min_rto, value))

    def update(self, sample_rtt):
        if self.srtt is None:
            self.srtt = sample_rtt
            self.rttvar = sample_rtt / 2
        else:
            self.rttvar = (1 - self.beta) * self.rttvar + self.beta * abs(sample_rtt - self.srtt)
            self.srtt = (1 - self.alpha) * self.srtt + self.alpha * sample_rtt
        self.rto = self._clamp(self.srtt + 4 * self.rttvar)

    def backoff(self):
        self.rto = min(self.max_rto, self.rto * 2)

    def reset_backoff(self):
        if self.srtt is None:
            self.rto = self.initial_rto
        else:
            self.rto = self._clamp(self.srtt + 4 * self.rttvar)


class WindowSender:
    # Part 2: window state and sending
    def __init__(self, packets, channel, sock, window_size):
        self.packets = packets
        self.channel = channel
        self.sock = sock
        self.N = window_size
        self.total = len(packets)

        self.base = 0
        self.next_seq = 0
        self.state = ["waiting"] * self.total
        self.send_time = [None] * self.total
        self.was_retransmitted = [False] * self.total

        self.estimator = RTOEstimator()
        self.timer_start = None
        self.last_ack = 0
        self.dup_acks = 0

        self.transmissions = 0
        self.retransmissions = 0
        self.timeouts = 0
        self.fast_retransmits = 0
        self.start_time = None
        self.end_time = None

        self.lock = threading.Lock()
        self.finished = threading.Event()

    def _send(self, seq, retransmit=False):
        self.channel.send(self.packets[seq], retransmit=retransmit)
        if self.next_seq >= self.total - 1:
            self.channel.flush()
        self.send_time[seq] = time.time()
        self.transmissions += 1
        if retransmit:
            self.retransmissions += 1
            self.was_retransmitted[seq] = True
            self.state[seq] = "retransmitted"
        else:
            self.state[seq] = "sent"

    def _fill_window(self):
        sent_any = False
        while self.next_seq < self.total and self.next_seq < self.base + self.N:
            self._send(self.next_seq)
            if self.timer_start is None:
                self.timer_start = time.time()
            self.next_seq += 1
            sent_any = True
        if sent_any:
            self._show_window()
        return sent_any

    # Part 3: ACKs, duplicate ACKs, fast retransmit
    def _ack_listener(self):
        while not self.finished.is_set():
            try:
                raw, _ = self.sock.recvfrom(4096)
                ptype, ack, _ = parse_packet(raw)
            except (socket.timeout, ConnectionResetError, ValueError):
                continue
            except OSError:
                break
            if ptype == TYPE_ACK:
                with self.lock:
                    self._handle_ack(ack)

    def _handle_ack(self, ack):
        if ack > self.base:
            newly_acked = range(self.base, ack)
            if any(self.was_retransmitted[s] for s in newly_acked):
                self.estimator.reset_backoff()
            else:
                self.estimator.update(time.time() - self.send_time[ack - 1])

            for s in newly_acked:
                self.state[s] = "acked"
            self.base = ack
            self.last_ack = ack
            self.dup_acks = 0
            log(f"ACK {ack} (timeout now {self.estimator.rto * 1000:.0f} ms)")

            if self.base == self.total:
                self.timer_start = None
                self.end_time = time.time()
                self.finished.set()
                return

            self.timer_start = time.time() if self.base < self.next_seq else None
            if not self._fill_window():
                self._show_window()

        elif ack == self.last_ack:
            self.dup_acks += 1
            log(f"DUP ACK {ack} (#{self.dup_acks})")
            if self.dup_acks == 3:
                log(f"3 DUP ACKS -> RETRANSMIT {self.base}")
                self.fast_retransmits += 1
                self._send(self.base, retransmit=True)
                self.timer_start = time.time()
                self._show_window()

    # Part 4: main loop and timeouts
    def run(self):
        self.start_time = time.time()
        threading.Thread(target=self._ack_listener, daemon=True).start()

        with self.lock:
            self._fill_window()

        while not self.finished.wait(0.01):
            with self.lock:
                if self.timer_start is None or self.finished.is_set():
                    continue
                if time.time() - self.timer_start >= self.estimator.rto:
                    self.timeouts += 1
                    log(f"TIMEOUT {self.base} -> RETRANSMIT {self.base}")
                    self._send(self.base, retransmit=True)
                    self.estimator.backoff()
                    self.timer_start = time.time()
                    self.dup_acks = 0
                    self._show_window()

        self._print_summary()

    # Part 5: display and summary
    def _show_window(self):
        if self.base >= self.total:
            return
        last = min(self.base + self.N, self.total) - 1
        cells = "  ".join(f"{s}:{self.state[s]}" for s in range(self.base, last + 1))
        log(f"WINDOW [{self.base}-{last}]  {cells}")

    def _print_summary(self):
        elapsed = self.end_time - self.start_time
        print("=" * 60)
        print(" SENDER SUMMARY")
        print(f" Packets (incl. END)  : {self.total}")
        print(f" Transmissions        : {self.transmissions}")
        print(f" Dropped (simulated)  : {self.channel.dropped}")
        print(f" Reordered (simulated): {self.channel.reordered}")
        print(f" Retransmissions      : {self.retransmissions}")
        print(f"   after timeout      : {self.timeouts}")
        print(f"   fast retransmit    : {self.fast_retransmits}")
        print(f" Timeouts             : {self.timeouts}")
        print(f" Final timeout (RTO)  : {self.estimator.rto * 1000:.0f} ms")
        print(f" Transfer time        : {elapsed:.3f} s")
        print(" File check           : MATCH/MISMATCH shown on the receiver")
        print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Reliable file transfer over UDP - sender")
    parser.add_argument("file")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--window", type=int, default=8)
    parser.add_argument("--loss", type=float, default=0.1)
    parser.add_argument("--reorder", type=float, default=0.1)
    parser.add_argument("--chunk", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    prepared = PreparedFile(args.file, args.chunk)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.2)
    channel = UnreliableChannel(sock, (args.host, args.port), args.loss, args.reorder,
                                seed=args.seed, log=log)

    print(format_settings(prepared, args.window, args.loss, args.reorder))
    WindowSender(prepared.packets, channel, sock, args.window).run()
    sock.close()


if __name__ == "__main__":
    main()
