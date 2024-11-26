import config
from datetime import datetime
from pathlib import Path
import pandas as pd
import praw
import time
import os
import logging
import requests
import sys
sys.path.append('../../')


def setup_client():
    # Set up API credentials
    reddit = praw.Reddit(
            client_id=config.REDDIT_CLIENT_ID,
            client_secret=config.REDDIT_SECRET,
            user_agent=config.REDDIT_USER_AGENT,
        )
    return reddit


def setup_logging(method, log_file_prefix: str = "get_reddit_data"):
    """Configure logging for the script."""
    log_file = (
        f"../logs/{log_file_prefix}_{method}_{datetime.now().strftime('%Y_%m_%d')}.log"
    )
    logging.basicConfig(
            filename=log_file,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )
    logging.info(f"Logging started for {method} method.")


def download_image(post, img_dir, subreddit_name):

    download_status = "todo"  # set default status
    try:
        response = requests.get(post.url)
        if response.status_code == 200:  # Successful request
            # Save the image
            file_name = f"{img_dir}/{subreddit_name}_{post.id}.jpg"
            
            with open(file_name, 'wb') as f:
                f.write(response.content)
                logging.info(f"Downloaded: {post.url}")
                download_status = "success"
        else:
            download_status = "failed"
    except Exception as e:
        logging.error(e)
    return download_status


def setup_subreddits(method):  # method = 'stream' or 'hot'

    if method == 'stream':  # combined subreddits as string
        subreddits = "conspiracy+Wikileaks+EndlessWar+collapse+postcollapse+SocialEngineering+propaganda+DescentIntoTyranny+MilitaryConspiracy+Bad_Cop_No_Donut+Palestine+Missing411+israelexposed+911truth+politic+conspiracyfact+permaculture+conspiracydocumentary+politics1+unitedwestand+TheSurvivalGuide+altnewz+Intelligence+NSALeaks+falseflagwatch+privacy+Subliminal+activism+UNAgenda21+ufos+Paranormal+C_S_T+naturalremedies+Shills+RomeRules+conspiracymemes+Psychonaut+fringetheory"

    elif method == 'hot':  # list of individual subreddits
        df = pd.read_csv('conspiracy_subreddits.csv')
        subreddits = list(df.subreddit_name.values)
    return subreddits


def create_post(post, download_status="no image"): 
    return {
                "subreddit": post.subreddit.display_name,
                "post_id": post.id,
                "title": post.title,
                "selftext": post.selftext,
                "created_utc": datetime.fromtimestamp(post.created_utc),
                "author": post.author.name if post.author else "Deleted",
                "score": post.score,
                "num_comments": post.num_comments,
                "url": post.url,
                "flair": post.link_flair_text,
                "download_status": download_status
            }


def save_results(data_dir, posts_data, counter, subreddit_name=None):
    df = pd.DataFrame(posts_data)
    logging.info(f"{counter} posts collected.")
    if subreddit_name:
        f_out = data_dir / f"{subreddit_name}_{datetime.now().strftime('%Y_%m_%d')}_{counter}.csv"
    else:
        f_out = data_dir / f"{datetime.now().strftime('%Y_%m_%d')}_{counter}.csv"
    df.to_csv(f_out, index=False)


def get_reddit_messages(method, data_dir, limit=None):
    img_dir = data_dir / 'images'
    subreddits = setup_subreddits(method)  # get single string or list of subreddits
    setup_logging(subreddits, method)  # set up logging
    reddit = setup_client()  # set up reddit client

    if method == 'stream':
        subreddit_name = subreddits  # single string combining all subreddits
        logging.info(f"STARTING SCRIPT TO COLLECT DATA FROM {subreddit_name}")
        posts_data = []
        subreddit = reddit.subreddit(subreddit_name)
        logging.info(f"Getting data from subreddit: {subreddit.display_name}")

        for post in subreddit.submissions.stream():  # use submissions.stream() to get a stream of posts
            counter = 0 # counter to keep track of number of posts collected
            download_status = "no image"
            # download image if post.url is an image
            if post.url.endswith(('jpg', 'png', 'jpeg')):
                download_status = download_image(post, img_dir, post.subreddit.display_name)
                # TODO GALLERY IMAGES

            # Append relevant post data to list
            post = create_post(post, download_status)
            posts_data.append(post)

            counter += 1
        if counter % 100 == 0:
            save_results(data_dir, posts_data, counter)
            posts_data = []  # Reset posts_data to collect new data
        time.sleep(1)

    elif method == 'hot':
        
        for subreddit_name in subreddits:
            logging.info(f"STARTING SCRIPT TO COLLECT DATA FROM {subreddit_name}")
            try: 
                subreddit = reddit.subreddit(subreddit_name)
                logging.info(f"Getting data from subreddit: {subreddit.display_name}")

                posts_data = []
                counter = 0 # counter to keep track of number of posts collected
                for post in subreddit.hot(limit=limit):
                    download_status = "no image"
                    # download image if post.url is an image
                    if post.url.endswith(('jpg', 'png', 'jpeg')):
                        download_status = download_image(post, img_dir, post.subreddit.display_name)
                    if 'gallery' in post.url:
                        download_status = 'gallery'
                    # Append relevant post data to list
                    post = create_post(post, download_status)
                    posts_data.append(post)
                    counter += 1
                print(counter)
                save_results(data_dir, posts_data, counter, subreddit.display_name)
                logging.info(f"Collected {len(posts_data)} posts from r/{subreddit.display_name}.")

            except Exception as e:
                logging.info(f"Failed to get subreddit: {subreddit}")
                logging.info(e)
                print(f"Failed to get subreddit: {subreddit}")
                print(e)
        time.sleep(5)  # sleep 5 seconds after each subreddit

