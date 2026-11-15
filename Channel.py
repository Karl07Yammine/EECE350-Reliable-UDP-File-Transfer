import os
import random
import threading

from packets import parse_packet, TYPE_END

os.system("")   

RESET = "\033[0m"
COLORS = {
    "SEND": "\033[32m",         
    "DROPPED": "\033[31m",      
    "REORDERED": "\033[35m",    
    "FLUSH": "\033[33m",        
}


def console_log(message):
    
    color = COLORS.get(message.split(" ", 1)[0], "")
    print(f"{color}{message}{RESET}")


def _label(raw):
    
    ptype, seq, _ = parse_packet(raw)
    return f"END {seq}" if ptype == TYPE_END else str(seq)


class UnreliableChannel:
    def __init__(self, sock, dest_addr, loss=0.0, reorder=0.0,
                 seed=None, log=console_log):
        
        if not (0.0 <= loss <= 1.0 and 0.0 <= reorder <= 1.0):
            raise ValueError("loss and reorder must be between 0 and 1")
        self.sock = sock
        self.dest = dest_addr
        self.loss = loss
        self.reorder = reorder
        self.log = log
        self._rng = random.Random(seed)      
        self._lock = threading.RLock()
        self._held = None                    
        self.handled = 0           
        self.sent = 0              
        self.dropped = 0           
        self.reordered = 0         
        self.retransmits_seen = 0  

        def send(self, raw, retransmit=False):
        
        with self._lock:
            self.handled += 1
            if retransmit:
                self.retransmits_seen += 1
            label = _label(raw)

                        if self._rng.random() < self.loss:
                self.dropped += 1
                self.log(f"DROPPED {label} (simulated)")
                return "dropped"
            
            if self._held is None and self._rng.random() < self.reorder:
                self._held = (raw, label)
                return "held"          

            self._transmit(raw, label, retransmit)

            if self._held is not None:
                held_raw, held_label = self._held
                self._held = None
                self._transmit(held_raw, held_label, False, quiet=True)
                self.reordered += 1
                self.log(f"REORDERED {held_label} after {label}")
            return "sent"

    def flush(self):
        
        with self._lock:
            if self._held is not None:
                raw, label = self._held
                self._held = None
                self._transmit(raw, label, False, quiet=True)
                self.log(f"FLUSH {label} (no later packet to reorder with)")

    def _transmit(self, raw, label, retransmit, quiet=False):
        self.sock.sendto(raw, self.dest)
        self.sent += 1
        if not quiet:
            self.log(f"SEND {label}" + (" (retransmission)" if retransmit else ""))

    def stats(self):
        
        with self._lock:
            return {
                "handled": self.handled,
                "sent_on_wire": self.sent,
                "dropped": self.dropped,
                "reordered": self.reordered,
                "retransmissions_seen": self.retransmits_seen,
            }
