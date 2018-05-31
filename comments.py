import praw
from datetime import datetime
import requests
import shutil
from pprint import pprint


def transform_created(created_timestamp):
    created_date = datetime.utcfromtimestamp(created_timestamp)
    return created_date.strftime('%Y-%m-%d %H:%M:%S')


def recur_comment_thread(comment, thread_comments=None, parent=None):
    if thread_comments is None:  # gotcha! all_comments=[] as default parameter
        thread_comments = []     # only get evaluated when function is defined.
    comment_dict = {
        'author': '[deleted]' if not comment.author else comment.author.name,
        'text': comment.body,
        'parent': parent,
        'upvotes': comment.ups,
        'subreddit': comment.subreddit.display_name,
        'submission_id': comment.submission.id,
        'created': transform_created(comment.created_utc),
        'id': comment.id
        }
    thread_comments.append(comment_dict)
    for reply in comment.replies:
        recur_comment_thread(
                reply, thread_comments, parent=comment.id)
    return thread_comments


def get_submission_comments(submission):
    all_submission_comments = []
    for root_comment in submission.comments:
        all_submission_comments.extend(recur_comment_thread(root_comment))
    return all_submission_comments


def search_method(subreddit):
    return subreddit.new(limit=10)


def get_submission_data(subm):
    subm_data = {
        'author': '[deleted]' if not subm.author else subm.author.name,
        'title': subm.title,
        'url': subm.url if '/comments/' not in subm.url else None,
        'text': subm.selftext if subm.selftext else None,
        'subreddit': subm.subreddit.display_name,
        'id': subm.id,
        'created': transform_created(subm.created_utc),
        'upvotes': subm.ups,
        }
    return subm_data


def download_image(image_url, path):
    resp = requests.get(image_url, stream=True)
    if resp.status_code == 200:
        with open(path, 'wb') as f:
            resp.raw.decode_content = True
            shutil.copyfileobj(resp.raw, f)
        return True
    else:
        return False


#TODO: UNFINISHED
def get_image(subm_url):
    extensions = ['.gif', '.png', '.jpg', '.bmp', '.tif', '.svg']
    url_low = subm_url.lower()
    if any(url_low().endswith(e) for e in extensions):
        pass
    elif 'imgur.com' in url_low:
        url_low += '.jpg'
        pass
    else:
        False, None


if __name__ == '__main__':
    reddit = praw.Reddit(
        client_id=,  # Fill out
        client_secret=,  # Fill out
        user_agent="terst123")
    subreddit = reddit.subreddit('chile')
    submissions = search_method(subreddit)
    for subm in submissions:
        pprint(get_submission_data(subm))
        pprint(get_submission_comments(subm))
        print()
