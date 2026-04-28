import os

CHECKPOINT_FILE = "checkpoint.txt"

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE) as f:
        return set(f.read().splitlines())


def save_checkpoint(processed_files):
    with open(CHECKPOINT_FILE, "a") as f:
        for p in processed_files:
            f.write(p + "\n")