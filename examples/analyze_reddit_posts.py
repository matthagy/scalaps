"""
Examples of using scalaps to analyze a sample of Reddit posts.

Derived from a Scala tutorial for working with the same data:
https://towardsdatascience.com/interactively-exploring-reddit-posts-using-basic-scala-in-your-browsers-f394843069de
"""

import urllib.request
from collections import namedtuple

from scalaps import ScSeq

with urllib.request.urlopen('https://matthagy.com/RS_2018-01-sample.csv') as response:
    text = response.read().decode()

print(len(text), 'characters')
# > 933177 characters
print()

Post = namedtuple('Post', ['subreddit', 'author', 'title', 'score'])


def parse_post(line: str) -> Post:
    subreddit, author, title, score = line.split(',')
    return Post(subreddit, author, title, int(score))


posts = ScSeq(text.strip().split('\n')).map(parse_post).to_frozen_list()
print(posts.length, 'posts')
# > 11347 posts
print()

# Count the number of posts in AskReddit
n_ask_reddit = posts.filter(lambda p: p.subreddit == 'AskReddit').count()
print(n_ask_reddit, 'posts in AskReddit')
# > 235 posts in AskReddit
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

# > ('AskReddit', 235)
# > ('AutoNewspaper', 230)
# > ('RocketLeagueExchange', 90)
# > ('The_Donald', 77)
# > ('CryptoCurrency', 74)

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

    print('')


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

# > AutoNewspaper w/ 2070 total words and 1305 unique words
# >  herald: 0.02271
# >  times: 0.02077
# >  miami: 0.01401
# >  post: 0.0087
# >  washington: 0.00773
# >
# > AskReddit w/ 1943 total words and 879 unique words
# >  you: 0.0736
# >  your: 0.03294
# >  would: 0.01698
# >  how: 0.01441
# >  or: 0.01287
# >
# > newsbotbot w/ 607 total words and 510 unique words
# >  rt: 0.00988
# >  about: 0.00824
# >  year: 0.00824
# >  his: 0.00659
# >  an: 0.00659
# >
# > GlobalOffensiveTrade w/ 558 total words and 208 unique words
# >  keys: 0.05376
# >  ft: 0.05197
# >  doppler: 0.04301
# >  mw: 0.03047
# >  bayonet: 0.02867
# >
# > The_Donald w/ 556 total words and 434 unique words
# >  trump: 0.01619
# >  you: 0.01259
# >  twitter: 0.00899
# >  about: 0.00899
# >  they: 0.00899
# >
# > SteamTradingCards w/ 532 total words and 80 unique words
# >  sets: 0.09398
# >  up: 0.08835
# >  level: 0.08835
# >  bot: 0.07519
# >  selling: 0.04323
# >
# > Showerthoughts w/ 511 total words and 341 unique words
# >  i: 0.01761
# >  people: 0.01566
# >  would: 0.01174
# >  how: 0.01174
# >  when: 0.01174
# >
# > UMukhasimAutoNews w/ 492 total words and 394 unique words
# >  de: 0.03862
# >  le: 0.02033
# >  les: 0.01423
# >  la: 0.01423
# >  at: 0.01423
# >
# > RocketLeagueExchange w/ 469 total words and 220 unique words
# >  offers: 0.04691
# >  keys: 0.04264
# >  octane: 0.02772
# >  tw: 0.02772
# >  black: 0.02772
# >
# > CryptoCurrency w/ 461 total words and 343 unique words
# >  i: 0.01952
# >  cryptocurrency: 0.01518
# >  market: 0.01302
# >  no: 0.01085
# >  coins: 0.01085
# >
