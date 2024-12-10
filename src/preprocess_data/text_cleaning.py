import re


def extract_hashtags(text):
    """Extracts all hashtags separately. Additionally, split CamelCase hashtags into single words."""    
    hashtag_pattern = re.compile(r'#\w+')
    
    def replace_hashtag(match):
        hashtag = match.group().lstrip('#')
        # Insert space before capital letters in camel case words
        processed_hashtag = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', hashtag)
        return processed_hashtag if processed_hashtag != '' else ' '

    cleaned_text = re.sub(hashtag_pattern, lambda match: replace_hashtag(match), text)
    return cleaned_text


def remove_urls(text):
    """Replace every URL by the empty string"""
    url_pattern = re.compile(r'https?://\S+|www\.\S+')
    cleaned_text = re.sub(url_pattern, '', text)
    return cleaned_text


def remove_mentions(text): 
    cleaned_text = re.sub("@\S+", " ", text)
    return cleaned_text


def remove_whitespace(text):
    cleaned_text = text.replace("   ", " ")
    cleaned_text = cleaned_text.replace("  ", " ")
    return cleaned_text


def clean_text(text):
    x = remove_urls(text)
    x = extract_hashtags(x)
    x = remove_mentions(x)
    x = remove_whitespace(x)
    return x
