from image_hashing import build_media_path, hash_images
from text_cleaning import clean_text
import sys
sys.path.append('../../')
from src.helpers import create_postgres_engine, execute_sql_select
from sqlalchemy import String, DateTime
# from src.config import HOST, PORT, USER, PW


if __name__ == '__main__':
    # Load data
    database = 'telegram'
    table_name_in = 'telegram_2024_10_11_raw'
    query = f"SELECT * FROM {table_name_in}"
    df = execute_sql_select(query, database, return_result_as_df=True)
    # Clean text
    df['cleaned_text'] = df['text'].apply(lambda x: clean_text(x) if x is not None else None)
    # Create media path and hash images
    data_dir = '../data/telegram/oct_nov_24/'
    df['file_path'] = df.apply(build_media_path, data_dir=data_dir, axis=1)
    df['dhash'], df['phash'] = zip(*df.apply(hash_images, axis=1))
    # Write cleaned data to DB
    eng = create_postgres_engine(database="telegram")
    dtype = dtype = {
        'channel_id': String,
        'message_id': String,
        'custom_id': String,
        'via_bot_id': String,
        'msg_date': String,
        'edit_date': String,
        'media_id': String,
        'access_hash': String,
        'start_date': String,
        'end_date': String,
        'msg_date_dt': DateTime,
        'dhash': String,
        'phash': String
    }
    table_name_out = 'telegram_2024_10_11_cleaned'
    #  df.to_sql(table_name_out, eng, if_exists='fail', dtype=dtype, index=False)  
    print(f'Data saved to {table_name_out}.')