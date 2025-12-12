#!/usr/bin/env python3
"""
update_db.py
Fetches output roots for requests in the database from a given start_block onward,
and updates each request’s preroot and postroot fields.
Usage:
    python3 update_db_roots.py --start-block 2049571
Requires:
    - psycopg2
    - requests
"""

import argparse
import psycopg2
import requests

# DB connection config
DB_CONFIG = {
    'dbname': 'op-succinct',
    'user': 'op-succinct',
    'host': 'localhost',
    'port': 5432
}

# L2 node URL
L2_NODE_URL = "http://localhost:37545"


def to_hex(n):
    """
    Convert integer to hex string prefixed with '0x'.
    """
    return hex(n)


def fetch_output_root(block_number):
    """
    Query the L2 node for the outputRoot at a given block number.
    Returns the hex string (e.g. '0xabc123...') or None on error.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "optimism_outputAtBlock",
        "params": [to_hex(block_number)]
    }
    headers = {"Content-Type": "application/json"}

    try:
        resp = requests.post(L2_NODE_URL, json=payload, headers=headers)
        resp.raise_for_status()
        return resp.json().get("result", {}).get("outputRoot")
    except Exception as e:
        print(f"[ERROR] fetching output root for block {block_number}: {e}")
        return None


def hex_to_bytes(hex_str):
    """
    Convert a hex string (with or without '0x') to bytes.
    """
    if not hex_str:
        return None
    clean = hex_str[2:] if hex_str.startswith("0x") else hex_str
    return bytes.fromhex(clean)


def parse_args():
    """
    Parse and return command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Update requests.preroot and requests.postroot for blocks >= start_block"
    )
    parser.add_argument(
        "--start-block", "-s",
        type=int,
        required=True,
        help="Only process requests with start_block >= this value"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    start_block_threshold = args.start_block

    # Connect to the database
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Fetch all requests at status=4 from the given start_block onward
    cur.execute("""
        SELECT id, start_block, end_block
        FROM requests
        WHERE start_block >= %s
          AND status = 4
    """, (start_block_threshold,))
    rows = cur.fetchall()

    print(
        f"Found {len(rows)} rows with start_block >= {start_block_threshold} and status = 4")

    for row_id, start_block, end_block in rows:
        # Fetch the output roots
        preroot_hex = fetch_output_root(start_block)
        postroot_hex = fetch_output_root(end_block)

        # Convert hex to bytes
        preroot = hex_to_bytes(preroot_hex)
        postroot = hex_to_bytes(postroot_hex)

        if preroot and postroot:
            # Update the row
            cur.execute("""
                UPDATE requests
                SET preroot = %s, postroot = %s
                WHERE id = %s
            """, (preroot, postroot, row_id))
            print(
                f"[OK]   Updated id={row_id}: preroot={preroot_hex}, postroot={postroot_hex}")
        else:
            print(
                f"[SKIP] id={row_id}: missing output_root for start or end block")

    # Commit changes and clean up
    conn.commit()
    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
