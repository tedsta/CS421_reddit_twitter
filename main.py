#!/usr/bin/env python3

import datetime
import itertools
import praw
import sqlite3

def flatten(listOfLists):
    "Flatten one level of nesting"
    return itertools.chain.from_iterable(listOfLists)

def build_database(db):
    db.execute("CREATE TABLE word(word TEXT PRIMARY KEY, use_count INTEGER);")
    
    db.execute("CREATE TABLE user(name TEXT PRIMARY KEY, site TEXT);")
    
    db.execute(
        "CREATE TABLE post("\
            "id TEXT PRIMARY KEY,"\
            "author TEXT,"
            "content TEXT,"\
            "datetime TEXT"\
        ");"
    )
    
    db.execute(
        "CREATE TABLE twitter_post("\
             "id TEXT PRIMARY KEY,"\
             "hashtags TEXT,"\
             "FOREIGN KEY(id) REFERENCES post(id)"\
         ");"
    )
    
    db.execute(
        "CREATE TABLE reddit_post("\
            "id TEXT PRIMARY KEY,"\
            "subreddit TEXT,"\
            "FOREIGN KEY(id) REFERENCES post(id)"\
        ");"
    )
    
    db.execute(
        "CREATE TABLE user_word("\
            "user TEXT,"\
            "word TEXT,"\
            "PRIMARY KEY(user, word),"\
            "FOREIGN KEY(user) REFERENCES user(name),"\
            "FOREIGN KEY(word) REFERENCES word(word)"\
        ");"
    )

    db.execute(
        "CREATE TABLE post_word("\
            "post TEXT,"\
            "word TEXT,"\
            "PRIMARY KEY(post, word),"\
            "FOREIGN KEY(post) REFERENCES post(id),"\
            "FOREIGN KEY(word) REFERENCES word(word)"\
        ");"
    )

####################################################################################################

def populate_word(db, words):
    for word in words:
        db.execute("INSERT INTO word values (?, ?);", word)

def populate_user(db, users):
    for user in users:
        db.execute("INSERT INTO user values (?, ?);", user)

def populate_post(db, posts):
    for post in posts:
        db.execute("INSERT INTO post values (?, ?, ?, ?);", post)

def populate_reddit_post(db, reddit_posts):
    for reddit_post in reddit_posts:
        db.execute("INSERT INTO reddit_post values (?, ?);", reddit_post)

def populate_twitter_post(db, twitter_posts):
    for twitter_post in twitter_posts:
        db.execute("INSERT INTO twitter_post values (?, ?);", twitter_post)

def populate_user_word(db, user_words):
    for user_word in user_words:
        db.execute("INSERT INTO user_word values (?, ?);", user_word)

def populate_post_word(db, post_words):
    for post_word in post_words:
        db.execute("INSERT INTO post_word values (?, ?);", post_word)

####################################################################################################

def reddit_posts_from_user(db, user):
    db.execute('SELECT content '\
               'FROM post '\
               'WHERE post.author = ?;', (user,))
    return [tuple(row) for row in db.fetchall()]

####################################################################################################

def main():
    # Data setup
    post_data = []
    
    # Connect to reddit
    user_agent = ("CS421 reddit crawler")
    r = praw.Reddit(user_agent=user_agent)

    subreddit = r.get_subreddit('rust')
    for submission in subreddit.get_hot(limit=500):
        if not submission.author:
            print("Skipping post with no author: "+submission.title.lower())
            continue
    
        id = submission.id
        title = submission.title.lower()
        subreddit = 'rust'
        author = submission.author.name
        
        post_datetime = datetime.datetime.utcfromtimestamp(submission.created_utc)
        year = datetime.datetime.strftime(post_datetime, "%Y")
        month = datetime.datetime.strftime(post_datetime, "%m")
        day = datetime.datetime.strftime(post_datetime, "%d")
        hour = datetime.datetime.strftime(post_datetime, "%H")
        minute = datetime.datetime.strftime(post_datetime, "%M")
        
        date_time = '-'.join([month, day, year, ':'.join([hour, minute])])
        
        # Add the post
        post_data.append((id, title, subreddit, author, date_time))
    
    # Build tables
    users = set([(post[3], "reddit") for post in post_data])
    
    words = {}
    user_word = set()
    post_word = set()
    for post in post_data:
        for word in post[1].split():
            word = word.strip('!@#$%^&*(),./<>?[]{}:;\'"-_=+`~|\\')
            if len(word) == 0:
                continue
            if word in words:
                words[word] += 1
            else:
                words[word] = 1
            user_word.add((post[3], word))
            post_word.add((post[0], word))
    
    posts = [(post[0], post[3], post[1], post[4]) for post in post_data]
    reddit_posts = [(post[0], post[2]) for post in post_data]
    
    """with open("users.txt", "w") as file:
        file.write("\n".join([" ".join(list(user)) for user in users]))
    with open("words.txt", "w") as file:
        file.write("\n".join([" ".join([word, str(use_count)]) for word, use_count in words.items()]))
    with open("user_word.txt", "w") as file:
        file.write("\n".join([" ".join(list(uw)) for uw in user_word]))
    with open("post_word.txt", "w") as file:
        file.write("\n".join([" ".join(list(pw)) for pw in post_word]))
    with open("posts.txt", "w") as file:
        file.write("\n".join([" ".join(list(post)) for post in posts]))
    with open("reddit_posts.txt", "w") as file:
        file.write("\n".join([" ".join(list(reddit_post)) for reddit_post in reddit_posts]))
    
    exit()"""
    
    # Connect to sqlite
    db = sqlite3.connect(':memory:')
    db_cur = db.cursor()
    
    # Build and populate the database
    
    build_database(db_cur)
    
    populate_word(db_cur, [(word, use_count) for word, use_count in words.items()])
    populate_user(db_cur, users)
    populate_post(db_cur, posts)
    populate_reddit_post(db_cur, reddit_posts)
    populate_post_word(db_cur, post_word)
    populate_user_word(db_cur, user_word)
    
    print("\n".join([str(row) for row in reddit_posts_from_user(db_cur, "brson")]))
    
    db.commit()
    
    # Close DB connection
    db.close()

############################################

if __name__ == "__main__":
    main()