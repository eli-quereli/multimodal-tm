import yaml
import base64
from PIL import Image
import google.generativeai as genai
import pandas as pd
import time
import json
import sys
sys.path.append('../../')
from src.config import GOOGLE_API_KEY


def load_prompts(yaml_file):
    """Load prompt data from YAML file."""
    with open(yaml_file, "r") as file:
        return yaml.safe_load(file)


def encode_image_to_base64(image_path):
    """Convert the image to a base64 encoded string."""
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode("utf-8")


def build_prompt(row, modality, prompts):
    intro = prompts['intro']
    annotation_instructions_text = prompts['annotation_instructions']['text']
    annotation_instructions_img = prompts['annotation_instructions']['image']
    annotation_instructions_mm = prompts['annotation_instructions']['multimodal']

    if modality == 'text':
        prompt = f"""{intro} Your task is to annotate a given topic based on the topic representation and example texts. 
        Topic representation: {row['Representation']}
        Example texts: {row['Representative_Docs']}
    {annotation_instructions_text}
    """
        return prompt

    if modality == 'image':
        img = Image.open(row['Image_Path'])
        prompt = f"""{intro}
        Your task is to annotate a given topic based on example images.
        {annotation_instructions_img}
        """
        return prompt, img

    if modality == 'multimodal':
        img = Image.open(row['Image_Path'])
        prompt = f"""{intro}
        Your task is to annotate a given topic based on the topic representation, example texts, and example images.
        Topic representation: {row['Representation']}
        Example texts: {row['Representative_Docs']}
        {annotation_instructions_mm}
        """
        return prompt, img


def process_response(response):
    json_str = str(response.text).replace('```json', '').replace('```', '').strip()
    return json.loads(json_str)


def annotate_topics(df, modality, prompts):
    results = pd.DataFrame()
    for i, row in df.iterrows():
        try: 
            if modality == 'text':
                instruction = build_prompt(row, modality, prompts)
                response = model.generate_content([instruction])
                time.sleep(5) # respect rate limits
            else: 
                instruction, img = build_prompt(row, modality, prompts)
                # img.show()
                response = model.generate_content([instruction, img])
                time.sleep(5) # respect rate limits
            json_response = process_response(response)
            temp = pd.DataFrame([json_response])
            temp['topic_no'] = row['Topic']
            results = pd.concat([results, temp])
        except Exception as e:
            temp = pd.DataFrame(data=None, columns=results.columns)  # save an empty entry to ensure correct numbering of topics
            results = pd.concat([results, temp])
            print(e)
            time.sleep(60)
    return results


if __name__ == '__main__':

    # Pass the api key 
    genai.configure(api_key=GOOGLE_API_KEY)

    model = genai.GenerativeModel(model_name="gemini-1.5-flash")
    # Load prompts from the YAML file
    prompts = load_prompts("prompts/topic_annotation.yaml")

    modality = 'text'
    print(f'Annotating {modality} topics...')
    # text topics from October 2023, for evaluation
    # df = pd.read_csv('eval/october_23/text_topic_info.csv')

    # text topics from November 2024
    df = pd.read_csv('dedupe_text_20_nov_topic_info.csv')
    results = annotate_topics(df, modality, prompts)
    f_out = 'results/dedupe_text_20_nov.csv'
    results.to_csv(f_out, index=False)
    time.sleep(60)
    print('Done.')

    modality = 'image'
    print(f'Annotating {modality} topics...')
    # img topics from October 2023
    # df = pd.read_csv('eval/october_23/img_topic_info.csv')
    # img topics from November 2023
    df = pd.read_csv('dedupe_img_20_nov_topic_info_w_img_path.csv')
    results = annotate_topics(df, modality, prompts)    
    # f_out = 'eval/topic_annotations/img_v2.csv'
    f_out = 'results/dedupe_img_20_nov.csv'
    results.to_csv(f_out, index=False)
    time.sleep(60)
    print('Done.')

    # multimodal topics from October 2023
    modality = 'multimodal'
    print(f'Annotating {modality} topics...')
    # multimodal topics from October 2023
    # df = pd.read_csv('eval/october_23/mm_topic_info.csv')
    # mm topics from November 2023
    df = pd.read_csv('dedupe_mm_20_nov_topic_info_w_img_path.csv')
    results = annotate_topics(df, modality, prompts)
    # f_out = 'eval/topic_annotations/mm_v2.csv'
    f_out = 'results/dedupe_mm_20_nov.csv'
    results.to_csv(f_out, index=False)
    print('Done.')
