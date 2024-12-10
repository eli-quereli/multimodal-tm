import config
from random import randrange
from get_telegram_data import setup_logging, get_messages
import asyncio
import logging
import pandas as pd
import pytz
from datetime import datetime
from telethon import TelegramClient, errors
from pathlib import Path

import sys
sys.path.append('../../')


data_dir = Path('../data/telegram/oct_nov_24/')


def save_results(data, category, channel_name, start_date, end_date, data_dir):
    df = pd.DataFrame.from_records(data)
    df['category'] = category
    df['start_date'] = start_date.strftime('%Y_%m_%d')
    df['end_date'] = end_date.strftime('%Y_%m_%d')
    output_file = data_dir / f"{channel_name}_{datetime.now().strftime('%Y_%m_%d')}.csv"
    df.to_csv(output_file, index=False)
    logging.info(
        "RESULTS FOR CHANNEL %s SAVED TO %s",
        channel_name,
        output_file
        )


async def main():
    setup_logging()
    # Define date range for message collection
    start_date = datetime(2024, 11, 29, tzinfo=pytz.UTC)  # inclusive, i.e. >=
    end_date = datetime(2024, 12, 1, tzinfo=pytz.UTC)  # exclusive, i.e. <
    print(start_date, end_date, type(start_date), type(end_date))
    # load channel list and select channels by category
    channels = pd.read_csv("sources/telegram_ids_batch_1.csv")  # 1. Runde von get_telegram_channel_ids.py
    # channels = pd.read_csv("sources/telegram_channel_ids_2023_419.csv")  # 2. + 3. Runde von get_telegram_channel_ids.py
    channels = channels.query('channel_id != -1')

    # fetch messages from channels per category
    categories = list(channels.category.unique())
    print(f"Collecting messages for {len(categories)} categories: {categories}")
    
    for category in categories:
        print(f"STARTING SCRIPT FOR CATEGORY: {category}")
        logging.info("STARTING SCRIPT FOR CATEGORY: %s", category)
        channels_to_get = channels.query("category == @category")
        channels_to_get.reset_index(inplace=True)

        # start time for script, per category
        start = datetime.now()

        async with TelegramClient('session', config.TG_API_ID, config.TG_API_HASH) as client:
            for i, row in channels_to_get.iterrows():
                channel_name = row['channel_name']
                logging.info("FETCHING DATA FOR CHANNEL %s", channel_name)
                channel_id, access_hash = row['channel_id'], row['access_hash']

                # store channel info for channels_fetched 
                temp = pd.DataFrame()
                temp['channel_name'] = channel_name
                temp['category'] = category
                temp['channel_id'] = channel_id
                temp['access_hash'] = access_hash

                try:
                    data = await get_messages(
                        client,
                        channel_name,
                        channel_id,
                        access_hash,
                        start_date,
                        end_date,
                        data_dir
                    )
                    if data is not None:
                        save_results(
                            data,
                            category,
                            channel_name,
                            start_date,
                            end_date,
                            data_dir
                            )
                        temp['status'] = 'success'
                    else:
                        print(f"No data for {channel_name}")
                        logging.info(f"No data for {channel_name}.")
                        temp['status'] = 'no data'
                except errors.FloodWaitError as e:
                    logging.exception(f"FloodWaitError with channel: {channel_name}")
                    logging.exception(e)
                    logging.info(f'Have to sleep {e.seconds} seconds')
                    # sleep for required time + random amount of seconds (ca. 1 hour)
                    await asyncio.sleep(e.seconds + randrange(3300 + 300/1.2))
                except Exception as exc:
                    logging.exception(f"EXCEPTION OCCURED FOR CHANNEL: {channel_name}")
                    logging.exception(f"Exception: {exc}")
                    temp['status'] = 'failed'
                await asyncio.sleep(10 + randrange(20)/10)
        stop = datetime.now()
        logging.info(
            "SCRIPT FOR CATEGORY %s FINISHED AT %s",
            category, str(stop)
        )
        logging.info(f"TIME ELAPSED FOR SCRIPT: {str(stop-start)}")
        print(f"Time elapsed for script: {stop-start}")

if __name__ == "__main__":
    asyncio.run(main())
