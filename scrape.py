#!/usr/bin/env python
# -*- coding: utf-8 -*-

import praw
import pymysql
from dotenv import load_dotenv
import os
import sys
from functools import partial
from datetime import datetime
#from pprint import pprint


def transform_created(created_timestamp):
    created_date = datetime.utcfromtimestamp(created_timestamp)
    return created_date.strftime('%Y-%m-%d %H:%M:%S')


def recur_comment_thread(comment, thread_comments=None, parent=None):
    if thread_comments is None:  # gotcha! all_comments=[] as default parameter
        thread_comments = []     # only gets evaluated when function is defined
    if isinstance(comment, praw.models.reddit.more.MoreComments):
        for more_comment in comment.comments():
            recur_comment_thread(more_comment, thread_comments, parent)
    comment_dict = {
        'author': '[deleted]' if not comment.author else comment.author.name,
        'text': comment.body,
        'parent': parent,
        'upvotes': comment.ups,
        'subreddit': comment.subreddit.display_name,
        'submission_id': comment.submission.id,
        'created': transform_created(comment.created_utc),
        'id': comment.id,
        }
    thread_comments.append(comment_dict)
    for reply in comment.replies:
        recur_comment_thread(
                reply, thread_comments, comment.id)
    return thread_comments


def get_submission_comments(submission):
    all_submission_comments = []
    for root_comment in submission.comments:
        all_submission_comments.extend(recur_comment_thread(root_comment))
    return all_submission_comments


def search_method(subreddit):
    return subreddit.top('day', limit=25)


def get_submission_data(subm):
    subm_data = {
        'author': '[deleted]' if not subm.author else subm.author.name,
        'title': subm.title,
        'url': subm.url if '/comments/' not in subm.url else None,
        'text': subm.selftext if subm.selftext else None,
        'flair': subm.link_flair_text,
        'subreddit': subm.subreddit.display_name,
        'id': subm.id,
        'created': transform_created(subm.created_utc),
        'upvotes': subm.ups,
        }
    return subm_data


def ordered_dict_values(data_dict, *keys):
    ordered_values = []
    for k in keys:
        ordered_values.append(data_dict[k])
    return ordered_values


def db_insert_submission(subm_data, db_host, db_user, db_pass, db_name):
    connection = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            db=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
            )
    try:
        with connection.cursor() as cursor:
            select_query = ("SELECT * FROM submission WHERE %s = id LIMIT 1")
            cursor.execute(select_query, (subm_data['id']))
            db_subm = cursor.fetchall()
            if not db_subm:
                insert_query = ("INSERT INTO submission "
                                "    (id, title, text, url, flair, subreddit, "
                                "     upvotes, author, created) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
                cursor.execute(insert_query, (ordered_dict_values(
                    subm_data, 'id', 'title', 'text', 'url', 'flair',
                    'subreddit', 'upvotes', 'author', 'created')))
            else:
                update_query = ("UPDATE submission SET "
                                "        title = %s, text = %s, url = %s, "
                                "        flair = %s, upvotes = %s "
                                "WHERE id = %s")
                cursor.execute(update_query, (ordered_dict_values(
                    subm_data,
                    'title', 'text', 'url', 'flair', 'upvotes', 'id')))

        connection.commit()
    finally:
        connection.close()


def db_insert_comment(comm_data, db_host, db_user, db_pass, db_name):
    connection = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            db=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
            )
    try:
        with connection.cursor() as cursor:
            query = ("REPLACE INTO comment "
                     "             (id, submission_id, text, parent, "
                     "              subreddit, upvotes, author, created) "
                     "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
            cursor.execute(query, (ordered_dict_values(
                comm_data, 'id', 'submission_id', 'text', 'parent',
                'subreddit', 'upvotes', 'author', 'created')))
        connection.commit()
    finally:
        connection.close()


def db_create_tables(db_host, db_user, db_pass, db_name):
    connection = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            db=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
            )
    try:
        with connection.cursor() as cursor:
            fkc0_query = 'SET FOREIGN_KEY_CHECKS = 0'
            cursor.execute(fkc0_query)

            drop_query = 'DROP TABLE IF EXISTS submission'
            cursor.execute(drop_query)
            make_query = (
                    "CREATE TABLE submission ( "
                    "id VARCHAR(6) NOT NULL PRIMARY KEY, "
                    "title VARCHAR(555) NOT NULL, "
                    "text TEXT, "
                    "url VARCHAR(555), "
                    "flair VARCHAR(125), "
                    "subreddit VARCHAR(125) NOT NULL, "
                    "upvotes SMALLINT NOT NULL, "
                    "author VARCHAR(125) NOT NULL, "
                    "created DATETIME NOT NULL "
                    ") CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
                    )
            cursor.execute(make_query)

            drop_query = 'DROP TABLE IF EXISTS comment'
            cursor.execute(drop_query)
            make_query = (
                    "CREATE TABLE comment ( "
                    "id VARCHAR(7) NOT NULL PRIMARY KEY, "
                    "submission_id VARCHAR(6) NOT NULL, "
                    "text TEXT NOT NULL, "
                    "parent VARCHAR(7), "
                    "subreddit VARCHAR(125) NOT NULL, "
                    "upvotes SMALLINT NOT NULL, "
                    "author VARCHAR(125) NOT NULL, "
                    "created DATETIME NOT NULL, "
                    "FOREIGN KEY (submission_id) REFERENCES submission(id)"
                    ") CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
                    )
            cursor.execute(make_query)

            fkc1_query = 'SET FOREIGN_KEY_CHECKS = 1'
            cursor.execute(fkc1_query)
        connection.commit()
    finally:
        connection.close()


def main():

    load_dotenv(dotenv_path=os.path.realpath(__file__))

    CONFIG = {
            'CLIENT_ID': os.environ.get('REDDIT_ID'),
            'CLIENT_SECRET': os.environ.get('REDDIT_SECRET'),
            'DB_HOST': os.environ.get('DB_HOST'),
            'DB_USER': os.environ.get('DB_USER'),
            'DB_PASS': os.environ.get('DB_PASS'),
            'DB_NAME': 'reddit-latam',
            'USER_AGENT': 'Praw-bot'
            'SUBREDDITS': [
                'chile',
                'argentina',
                'mexico',
                'vzla',
                'peru',
                'colombia',
                'es',
                'uruguay'
                ]
            }

    for key in CONFIG.keys():
        if CONFIG[key] is None:
            sys.exit(key + ' not in environment variables... exiting.')

    save_submission = partial(
            db_insert_submission, db_host=CONFIG['DB_HOST'],
            db_user=CONFIG['DB_USER'], db_pass=CONFIG['DB_PASS'],
            db_name=CONFIG['DB_NAME'])

    save_comment = partial(
            db_insert_comment, db_host=CONFIG['DB_HOST'],
            db_user=CONFIG['DB_USER'], db_pass=CONFIG['DB_PASS'],
            db_name=CONFIG['DB_NAME'])

    wipe_db = partial(
            db_create_tables, db_host=CONFIG['DB_HOST'],
            db_user=CONFIG['DB_USER'], db_pass=CONFIG['DB_PASS'],
            db_name=CONFIG['DB_NAME'])

    reddit = praw.Reddit(client_id=CONFIG['CLIENT_ID'],
                         client_secret=CONFIG['CLIENT_SECRET'],
                         user_agent='test123')

    for sub_name in CONFIG['SUBREDDITS']:
        subreddit = reddit.subreddit(sub_name)
        submissions = search_method(subreddit)
        print('\n\n====[ {} ]===='.format(sub_name.upper()))
        for subm in submissions:
            subm_data = get_submission_data(subm)
            print('\n\n ->', subm_data['title'])
            save_submission(subm_data)
            subm_comments = get_submission_comments(subm)
            for comm in subm_comments:
                print('.', end='', flush=True)
                save_comment(comm)


if __name__ == '__main__':
    main()
