from pathlib import Path
from get_reddit_data import get_reddit_messages, setup_logging
import argparse
import logging
import time


def main():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "--method",
        type=str,
        help="Method used to collect Reddit posts. Options: hot, stream.",
        required=True
    )

    parser.add_argument(
        "--limit",
        type=int,
        help="Limit for number of posts to collect, only used for hot method.",
        required=False
    )
    args = parser.parse_args()

    data_dir_base = Path('../data/reddit/')
    data_dir = data_dir_base / args.method

    print("Setting up logging...")
    setup_logging(args.method)
    start = time.time()
    try:
        logging.info(f"Getting data using method {args.method.lower()}, writing results to: {data_dir}, limit: {args.limit}")
        get_reddit_messages(method=args.method, data_dir=data_dir, limit=args.limit)
    except Exception as e:
        print("Process failed.")
        print(e)
        logging.error("Process failed.")
        logging.error(e)
    stop = time.time()
    print('Done')
    logging.info(f"Time elapsed: {time.strftime('%H:%M:%S', time.gmtime(stop - start))}")


if __name__ == '__main__':
    main()