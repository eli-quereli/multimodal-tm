import sys
sys.path.append('../../')
import pandas as pd 
from bertopic import BERTopic
import base64
from io import BytesIO
from PIL import Image
import os


def image_base64(im):
    """
    Converts an image (PIL.Image or string path) to a Base64 string.
    """
    if isinstance(im, str):
        # Assume `im` is a file path and open it
        im = Image.open(im)
    elif not isinstance(im, Image.Image):
        raise TypeError("Input should be a PIL.Image.Image, or a file path as a string.")
    with BytesIO() as buffer:
        im.save(buffer, format='JPEG')
        return base64.b64encode(buffer.getvalue()).decode()


def image_formatter(im):
    """
    Converts an image to an HTML <img> tag with Base64 data.
    """
    return f'<img src="data:image/jpeg;base64,{image_base64(im)}">'


def extract_images(topic_model, output_dir):
    """
    Extracts images from the topic model's dataframe and saves them separately.
    
    Args:
        topic_model: The topic model object containing the dataframe.
        output_dir (str): Directory to save the extracted images.

    Returns:
        pandas.DataFrame: The dataframe with images saved and paths added.
    """
    os.makedirs(output_dir, exist_ok=True)
    # Extract dataframe
    topics_df = topic_model.get_topic_info().drop(columns=["Representative_Docs", "Name"], axis=1)
    # Process images
    image_paths = []
    for index, row in topics_df.iterrows():
        visual_aspect = row['Visual_Aspect']
        # Handle both Base64-encoded and direct PIL.Image.Image objects
        if isinstance(visual_aspect, str):
            # Decode Base64 image
            img_data = base64.b64decode(visual_aspect)
            img = Image.open(BytesIO(img_data))
        elif isinstance(visual_aspect, Image.Image):
            # Already a PIL Image
            img = visual_aspect
        else:
            raise TypeError("Unexpected type for 'Visual_Aspect'. Expected str or PIL.Image.Image.")
        # Save image to output directory
        img_path = os.path.join(output_dir, f"image_{index}.jpeg")
        img.save(img_path, format='JPEG')
        image_paths.append(img_path)
    # Add image paths to the dataframe
    topics_df['Image_Path'] = image_paths
    return topics_df


def extract_images_per_topic_model(model_name, mode, output_dir):
    print('Loading topic model.')
    topic_model = BERTopic.load(f'../models/{model_name}')
    print('Loading topic info.')
    results_df = pd.read_csv(f"../results/{mode}/{model_name}_topic_info.csv")
    # Extract images; the new df contains the paths to the saved images, image tiles are saved to output dir
    print('Extracting images.')
    img_file_df = extract_images(topic_model, output_dir=output_dir)
    # Merge topic info with image paths
    img_file_df = img_file_df[['Topic', 'Image_Path']]
    print("Merging dataframes.")
    merged_df = pd.merge(results_df, img_file_df, on='Topic')
    print(len(results_df) == len(merged_df))
    print("Saving to csv.")
    merged_df.to_csv(f'{model_name}_topic_info_w_img_path.csv', index=False)
    print("Done.")


if __name__ == '__main__':
    model_name = 'dedupe_mm_20_nov'
    extract_images_per_topic_model(model_name=model_name, mode='dedupe', output_dir=f"extracted_images/{model_name}")