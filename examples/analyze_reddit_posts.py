"""
Examples of using scalaps to analyze Reddit posts.

Derived from a Scala tutorial for working with the same data:
https://towardsdatascience.com/interactively-exploring-reddit-posts-using-basic-scala-in-your-browsers-f394843069de
"""

import urllib.request
from collections import namedtuple, Counter

from scalaps import ScSeq, ScDict

with urllib.request.urlopen('https://matthagy.com/RS_2018-01-sample.csv') as response:
    text = response.read().decode()

print(len(text), 'bytes')

Post = namedtuple('Post', ['subreddit', 'author', 'title', 'score'])


def parse_post(line: str) -> Post:
    subreddit, author, title, score = line.split(',')
    return Post(subreddit, author, title, int(score))


posts = ScSeq(text.strip().split('\n')).map(parse_post).to_frozen_list()
print(posts.length, 'posts')
print()

# Count the number of posts in AskReddit
n_ask_reddit = posts.filter(lambda p: p.subreddit == 'AskReddit').count()
print(n_ask_reddit, 'posts in AskReddit')
print()

# Count the number of posts for each subreddit and show the five highest volume ones
print('The five highest volume subreddits are:')
(posts
 .group_by('subreddit')
 .items()
 .map(lambda t: (t[0], t[1].length))
 .sort_by(1)
 .reverse()
 .take(5)
 .for_each(print))
print()

# Compute the frequency of title words in each subreddit
# and show the highest frequency words for the subreddits w/ the most words
stop_words = {"the", "a", "this", "that", "to", "is", "of",
              "in", "and", "are", "there", "for", "on", "do",
              "what", "by", "has", "with", "as", "if", "be",
              "just", "from"}


def generate_post_words(post: Post):
    for word in post.title.lower().split():
        if word.isalpha() and word not in stop_words:
            yield post.subreddit, word


def extract_word_counts(subreddit_word_counts):
    return (subreddit_word_counts
            .map(lambda subreddit_word_count: (subreddit_word_count[0][1], subreddit_word_count[1]))
            .to_dict())


def show_subreddit_word_counts(subreddit_word_counts):
    subreddit, word_counts = subreddit_word_counts
    total_word_count = word_counts.values().sum()
    print(subreddit, 'w/', total_word_count, 'total words and', word_counts.length, 'unique words')

    word_freqs = word_counts.map_values(lambda count: count / total_word_count)
    (word_freqs
     .items()
     .sort_by(1)
     .reverse()
     .take(5)
     .for_each(lambda word_freq: print(f'  {word_freq[0]}: {round(word_freq[1], 5)}')))

    print()


(posts
 .flat_map(generate_post_words)
 .value_counts()
 .items()
 .group_by(lambda subreddit_word_count: subreddit_word_count[0][0])
 .map_values(extract_word_counts)
 .items()
 .sort_by(lambda subreddit_word_counts: subreddit_word_counts[1].values().sum())
 .reverse()
 .take(10)
 .for_each(show_subreddit_word_counts))
