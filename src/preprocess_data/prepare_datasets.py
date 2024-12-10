import sys
sys.path.append('../../')
from src.helpers import create_postgres_engine, execute_sql_select
from sqlalchemy import String, DateTime


if __name__ == '__main__':
    # Load data
    database = 'telegram'
    table_name_in = 'telegram_2024_10_11_cleaned'
    query = f"SELECT * FROM {table_name_in}"
    df = execute_sql_select(query, database, return_result_as_df=True)

    # Select only messages with image and text data
    df = df.query('msg_type == "image"')
    df['cleaned_text'] = df['cleaned_text'].fillna('')
    df = df[df['cleaned_text'].str.len() > 0]
    # Dedupe data
    df = df.drop_duplicates('custom_id') # base dedupe on message level (channel_name + message_id)
    base_dedupe = df.drop_duplicates(subset=['cleaned_text', 'dhash'])
    text_dedupe = df.drop_duplicates(subset=['cleaned_text'])
    image_dedupe = df.drop_duplicates(subset=['dhash'])
    mm_dedupe = df.drop_duplicates('dhash').drop_duplicates('cleaned_text')
    print(f'Base dedupe: {base_dedupe.shape[0]}')
    print(f'Text dedupe: {text_dedupe.shape[0]}')
    print(f'Image dedupe: {image_dedupe.shape[0]}')
    print(f'MM dedupe: {mm_dedupe.shape[0]}')

    # Write deduped data to DB
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
    base_dedupe.to_sql('telegram_2024_base', eng, if_exists='fail', dtype=dtype, index=False)
    text_dedupe.to_sql('telegram_2024_text', eng, if_exists='fail', dtype=dtype, index=False)
    image_dedupe.to_sql('telegram_2024_img', eng, if_exists='fail', dtype=dtype, index=False)
    mm_dedupe.to_sql('telegram_2024_mm', eng, if_exists='fail', dtype=dtype, index=False)
    print('Data saved to DB.')