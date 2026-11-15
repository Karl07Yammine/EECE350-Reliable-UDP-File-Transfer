# Reliable File Transfer over UDP

**EECE 350 Computer Networks – Final Project, Group 1**

This project sends a file over UDP and still delivers it **complete and in order**. UDP does not guarantee delivery, so the program recovers from lost and reordered packets the way TCP does:

- a **sliding window** on the sender
- **cumulative ACKs** from the receiver
- **timeout retransmission**, with a TCP-style timeout estimate
- **fast retransmit** after 3 duplicate ACKs

A simulated **Unreliable Channel** on the sender side randomly drops packets and holds some back so they arrive out of order. When the transfer ends, the receiver compares the SHA-256 hash of the file it rebuilt with the hash the sender put in the END packet, and reports **MATCH** or **MISMATCH**.

It is written in Python 3 using the standard library only: `socket`, `struct`, `threading`, `time`, `random`, `hashlib`, `argparse`.

---

## Team members and contributions

| Member | Name | Files | Responsibilities |
|---|---|---|---|
| Member 1 | **Miryana Hammidi** | `packets.py`, `channel.py` | Reading the file and splitting it into chunks. Packet header format (`struct`, `"!BIH"`). Building and parsing packets. END packet carrying the SHA-256 hash of the file. The Unreliable Channel (Simulator): simulated loss, reordering and flush. Settings display at start. |
| Member 2 | **Ali Yassine** | `sender.py` | Sliding window of N packets (base, next packet, state of each packet). Sliding the window on cumulative ACKs. TCP-style timeout estimate (SRTT / RTTVAR / RTO with backoff). Timeout retransmission. Fast retransmit on 3 duplicate ACKs. Live window display, sender event log and sender summary. |
| Member 3 | **Karl Yammine** | `receiver.py` | Receiving on a UDP socket and reading headers. Cumulative ACKs covering in-order, out-of-order (buffer + duplicate ACK), gap-filling and duplicate packets. Putting the file back together and writing it to disk. SHA-256 check (MATCH / MISMATCH). Staying alive after END to re-ACK. Receiver event log and summary. |

Each file starts with a comment naming its author. Any AI help is noted in the header comment of the file where it was used.

---

## Files

```
packets.py    Packet format, build/parse functions, file splitting, END packet  (Member 1)
channel.py    Unreliable Channel simulator: loss and reordering                 (Member 1)
sender.py     Sliding window, timeouts, retransmission, sender display          (Member 2)
receiver.py   Receiver, cumulative ACKs, reassembly, SHA-256 check              (Member 3)
```

All four files must be in the **same folder**, because `sender.py`, `receiver.py` and `channel.py` import from `packets.py`.

---

## How to run

You need Python 3.8 or newer. There is nothing to install.

**1. Start the receiver first**, in one terminal:

```bash
python3 receiver.py --out received.bin
```

**2. Start the sender**, in a second terminal:

```bash
python3 sender.py myfile.bin --window 8 --loss 0.1 --reorder 0.1
```

When the transfer finishes, the sender prints its summary. The receiver re-ACKs for a few seconds, writes `received.bin`, then prints **MATCH** or **MISMATCH** and its own summary.

**Example for the demo**, with heavier loss and a fixed seed so the same drops happen every run:

```bash
python3 receiver.py --out received.bin
python3 sender.py myfile.bin --window 8 --loss 0.2 --reorder 0.2 --seed 11
```

On Windows, use `python` instead of `python3`.

---

## Settings

### Sender (`sender.py`)

| Setting | Default | Meaning |
|---|---|---|
| `file` | *(required)* | Path of the file to send. |
| `--host` | `127.0.0.1` | IP address of the receiver. |
| `--port` | `5000` | UDP port the receiver listens on. |
| `--window` | `8` | Window size **N**: the most packets that can be sent and still waiting for an ACK at the same time. |
| `--loss` | `0.1` | Probability (0 to 1) that the simulated channel **drops** a packet. |
| `--reorder` | `0.1` | Probability (0 to 1) that the simulated channel **holds a packet back** and sends it after the next packet, so it arrives out of order. |
| `--chunk` | `1024` | Bytes of file data in each DATA packet. |
| `--seed` | *(random)* | Seed for the channel's random numbers. Using the same seed gives the same drops and reorders every run. |

### Receiver (`receiver.py`)

| Setting | Default | Meaning |
|---|---|---|
| `--host` | `127.0.0.1` | Address to listen on. Use `0.0.0.0` to accept packets from another machine. |
| `--port` | `5000` | UDP port to listen on. It must match the sender's `--port`. |
| `--out` | `received_file` | Path where the received file is written. |
| `--idle-timeout` | `30` | Seconds without any packet before the receiver gives up. |
| `--linger` | `3` | Seconds the receiver stays alive after END to re-ACK anything retransmitted, in case its final ACK was lost. |
| `--no-color` | off | Turns off coloured output, e.g. when writing the log to a file. |

---

## Protocol

**Packet header (7 bytes)**, `struct` format `"!BIH"` in network byte order:

| Field | Size | Meaning |
|---|---|---|
| type | 1 byte | `1` = DATA, `2` = ACK, `3` = END |
| seq | 4 bytes | Sequence number (counts packets, not bytes) |
| length | 2 bytes | Number of data bytes after the header |

**Sequence numbers**

- DATA packets are numbered `0 … n-1`.
- The END packet has sequence number `n`. Its data is the 32-byte SHA-256 hash of the original file. END is sent, retransmitted and ACKed like any DATA packet.
- An ACK carries the sequence number of the **next packet the receiver expects** (a cumulative ACK). ACK `n+1` means everything, including END, has arrived.

**What the receiver does with each packet**

| Case | Action | Example log line |
|---|---|---|
| In order | Accept it, ACK the next one expected | `RECV DATA 12 in order → ACK 13` |
| Out of order (a gap) | Buffer it, send a duplicate ACK for the missing packet | `RECV DATA 15 out of order, buffered → DUP ACK 13` |
| Fills a gap | Accept it plus the buffered packets that now follow, send a cumulative ACK | `RECV DATA 13 fills gap (+2 buffered delivered) → ACK 16` |
| Duplicate | Discard it, re-send the current ACK | `RECV DATA 12 duplicate, discarded → RE-ACK 13` |

**What the sender does**

- It sends new packets while the window has room, and slides the window when a new cumulative ACK arrives.
- One timer runs for the oldest unacknowledged packet. On timeout, that packet is retransmitted and the timeout doubles (backoff).
- The timeout is estimated like TCP's: `SRTT = (1-α)·SRTT + α·RTT`, `RTTVAR = (1-β)·RTTVAR + β·|RTT-SRTT|`, `RTO = SRTT + 4·RTTVAR`, with α = 0.125 and β = 0.25. RTO is kept between 200 ms and 3 s. RTT samples from retransmitted packets are not used, following Karn's algorithm.
- On 3 duplicate ACKs, the missing packet is retransmitted right away (fast retransmit).
- Every packet, new or retransmitted, goes through the Unreliable Channel. ACKs from the receiver are sent back directly and are not dropped.

---

## Display

- **At start (sender):** file name and size, chunk size, number of packets, window size N, loss and reorder probabilities, SHA-256 of the file.
- **Live window (sender):** `WINDOW [14-21]  14:acked  15:sent  16:retransmitted ...`
- **Event log on both sides,** one coloured line per event:
  - Sender: `SEND`, `DROPPED (simulated)`, `REORDERED x after y`, `ACK`, `DUP ACK`, `TIMEOUT x -> RETRANSMIT x`, `3 DUP ACKS -> RETRANSMIT x`, `WINDOW [...]`
  - Receiver: `RECV ... in order / out of order / fills gap / duplicate` and the ACK it sent back.
- **Summary at the end:**
  - Sender: transmissions, dropped, reordered, retransmissions (after timeout and fast), timeouts, transfer time.
  - Receiver: packets received, out of order, gaps filled, duplicates, ACKs and duplicate ACKs sent, transfer time, and the **MATCH / MISMATCH** file check.

---

## Testing

The four files were tested together over localhost. Every run produced a received file identical to the original, with a MATCH result:

- small file (50 KB) with default settings
- heavy loss and reorder (0.3 / 0.3)
- no loss or reorder (0 / 0)
- a file whose size is an exact multiple of the chunk size
- an empty file (only the END packet is sent)
- a 1 MB file with N = 16
- the sender started before the receiver
