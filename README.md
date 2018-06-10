# reddit2mysql

Simple script to collect data form [Reddit](https://www.reddit.com) using [Praw](https://praw.readthedocs.io). Downloads defined subreddits top submissions with all comments. If submission or comments already exist in DB, they are replaced with the latest version.

## TODO

- add argparse at least for db wipe/create
- check that comment parents are correct (that the parent parameter in the comment recur function is local to each iteration)
- loading .env doesn't work as expected. still have to set env variables.
