
import time
from umap import UMAP
import pandas as pd
import numpy as np
import sys
sys.path.append('../../')
from src.helpers import execute_sql_select
from topic_modeling import text_topic_modeling, image_topic_modeling, multimodal_topic_modeling


if __name__ == '__main__':
    start_date = np.datetime64('2024-11-01')
    end_date = np.datetime64('2024-11-30')
    # Set parameters
    min_topic_sizes = [20, 10] # 50 
    mode = 'dedupe'  # base, dedup
    modality = 'img'  # text, img, mm
    month = 'nov'  # oct, nov
    # Load data
    if mode == 'base':  # base deduplication
        table_name = f'telegram_2024_{mode}'
    else:   # dataset deduplication by modality
        table_name = f'telegram_2024_{modality}'
    query = f"SELECT * FROM {table_name}"
    database = "telegram"
    data = execute_sql_select(command=query, database=database, return_result_as_df=True)
    df = data.copy()
    df = df[(df['msg_date_dt'] >= start_date) & (df['msg_date_dt'] <= end_date)]
    print(len(df), "rows loaded.")
    print(df.msg_date_dt.min(), df.msg_date_dt.max())
    # Set seed in UMAP model for reproducibility
    umap_model = UMAP(random_state=42)

    t0 = time.perf_counter()  
    # Select text documents
    docs = list(df.cleaned_text.values)
    # Select image files
    images = list(df.file_path.values)
    print(images[0])
    print(f"Starting topic modeling for {modality} data from {month}, mode: {mode}.")
    for min_topic_size in min_topic_sizes:
        model_version = f"{mode}_{modality}_{min_topic_size}_{month}"
        print(f"Generating topics with min_topic_size = {min_topic_size}.")
        # Select image files
        if modality == 'text':
            text_topic_modeling(docs, umap_model, min_topic_size, model_version)
        elif modality == 'img':
            image_topic_modeling(images, umap_model, min_topic_size, model_version)
        elif modality == 'mm':
            multimodal_topic_modeling(docs, images, umap_model, min_topic_size, model_version)
    print("Time elapsed: ", time.strftime("%H:%M:%S", time.gmtime((time.perf_counter() - t0))))