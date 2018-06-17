# reddit2mysql

Simple script to collect data form [Reddit](https://www.reddit.com) using [Praw](https://praw.readthedocs.io). Downloads defined subreddits top submissions with all comments. If submission or comments already exist in DB, they are replaced with the latest version.

To run as a cron job, execute from a script to set environment variables, like the following:

~~~~bash
#!/usr/bin/env bash

source .env
/path/to/python reddit2mysql.py
~~~~
