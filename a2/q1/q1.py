from os import path
import json
from json.decoder import JSONDecodeError
from pprint import pprint
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np


pd.set_option('display.max_columns', None)


# paths
rootdir = '/Users/jj/Code/sqtt'   # todo: change this
# rootdir = '/Users/jenny.jin/src/sqtt'   # todo: change this
inputdir = path.join(rootdir, 'a2/q1/input')
outputdir = path.join(rootdir, 'a2/q1/output')

# constants
TIMESTAMP_STR = 'timestamp_ms'
RETWEETED_STATUS_STR = 'retweeted_status'
CREATED_STR = 'created'
LEVEL_STR = 'level'
HASHTAG_STR = 'hashtag'
CITY_STR = 'city'
COUNTY_STR = 'county'
STATE_STR = 'state'

"""
1. combine tweets2~tweets5.json into a single file
Determine the number of tweets in 2
"""
tweets2_data = []

# It's weird that the output filename is the same as that of one of the input files.
output_fpath = path.join(outputdir, 'tweets2.json')


with open(output_fpath, 'w', encoding='utf8') as output_file_handle:
    for i in [2, 3, 4, 5]:
        input_fpath = path.join(inputdir, 'tweets%d.json' % i)

        print('Writing %s to %s...' % (input_fpath, output_fpath))

        with open(input_fpath, 'r', encoding='utf8') as input_file_handle:
            for line in input_file_handle.readlines():
                if line.strip():
                    # output_file_handle.writelines(line)
                    tweets2_data.append(json.loads(line))

print('Wrote %d lines to %s' % (len(tweets2_data), output_fpath))

# Determine the number of tweets in 1

tweets1_data = []

with open(path.join(inputdir, 'tweets1.json'), 'r', encoding='utf8') as f:
    for line in f.readlines():
        if line.strip():
            try:
                tweets1_data.append(json.loads(line))
            except JSONDecodeError:
                print('Error json-decoding line:', line)

print('There are %d lines in tweets%d.json' % (len(tweets1_data), 1))   # 82947
print('There are %d lines in tweets%d.json' % (len(tweets2_data), 2))   # 62334


"""
2. return a dict of acceptable tweets
"""

# read country names
valid_locations = set()
with open(path.join(inputdir, 'country_and_us_states_names.txt'), 'r', encoding='utf-8') as ifile:
    for line in ifile.readlines():
        valid_locations.add(line.strip().lower())

# read us cities and counties
with open(path.join(inputdir, 'zip_codes_states.csv'), 'r') as ifile:
    for line in ifile.readlines():
        if line.strip():
            _, _, _, city, _, county = line.split(',')
            valid_locations.add(city.strip().lower())
            valid_locations.add(county.strip().lower())

if '' in valid_locations:
    valid_locations.remove('')

print('Read %d eligible locations.' % len(valid_locations))


def get_hashtags(tweet, does_include_retweet):
    def _get_hashtags(d):
        return [t['text'] for t in d.get('entities', {}).get('hashtags') or []]

    if does_include_retweet:
        return _get_hashtags(tweet) or _get_hashtags(tweet.get(RETWEETED_STATUS_STR, {}))
    else:
        return _get_hashtags(tweet)


def process_tweet(tweet):
    """
    :return: whether the tweet is acceptable (bool), location (str), hashtags (list of strs)
    """

    def _encode_to_english(s, default=None):
        ENCODING = 'latin-1'
        try:
            return s.encode(ENCODING).decode(ENCODING)
        except UnicodeEncodeError:
            return default

    # hashtags in entities or retweeted_status
    hashtags = get_hashtags(tweet, does_include_retweet=True)

    # convert tags to English
    english_hashtags = filter(lambda x: x is not None,
                              [_encode_to_english(t, default=None) for t in hashtags])

    # has a timestamp
    has_timestamp = TIMESTAMP_STR in tweet

    # has a legit location
    location, is_loc_legit = is_location_legit(tweet, valid_locations)

    return english_hashtags and has_timestamp and is_loc_legit, \
           location, \
           english_hashtags


def is_location_legit(tweet, locations_candidates):
    """
    todo: this function needs some work
    :return: location (str), whether the location is legit (bool)
    """
    location = get_location(tweet)

    if not location:
        return None, False

    location_tokens = set([t.strip() for t in location.lower().split(',')])

    return location, bool(location_tokens.intersection(locations_candidates))


def get_location(tweet):
    return tweet.get('user', {}).get('location')


def clean_tweets(tweets):
    """
    Question 2.2:
    :return: acceptable tweets, locations_list, locations_set, hashtags_list, hashtags_set
    """
    acceptable_tweets = []
    locations_list = []
    hashtags_list = []

    for d in tweets:
        is_legit, location, cur_hashtags = process_tweet(d)

        if is_legit:
            acceptable_tweets.append(d)

            # we keep track of location and hashtags only when the tweet is eligible
            locations_list.append(location)
            hashtags_list += cur_hashtags

    print('%d/%d tweets are clean.\n%d locations.\n%d hashtags.' %
          (len(acceptable_tweets), len(tweets), len(locations_list), len(hashtags_list)))

    # todo: do we return UNIQUE locations and hashtags (i.e. set instead of lists)?
    return acceptable_tweets, \
           locations_list, set(locations_list), \
           hashtags_list, set(hashtags_list)

clean_tweets1, locations_tweets1, _, hashtags_tweets1, _ = clean_tweets(tweets1_data)
clean_tweets2, locations_tweets2, _, hashtags_tweets2, _ = clean_tweets(tweets2_data)

"""
3. extract the top n tweeted hashtags
"""
def top_n_items(items, n):
    """
    :return: n most-frequent items from a list
    """

    count_dict = {}     # {item: count}

    for item in items:
        count_dict[item] = count_dict.get(item, 0) + 1
    print(sorted(count_dict.items(), key=lambda t: -t[1])[:n])
    return [key for key, _ in sorted(count_dict.items(), key=lambda t: -t[1])[:n]]

# todo: what is n? Use 20 for now.
n = 20
print('Top %d tweeted hashtags of tweets1.json:' % n)
pprint(top_n_items(hashtags_tweets1, n))
print('Top %d tweeted hashtags of tweets2.json:' % n)
pprint(top_n_items(hashtags_tweets2, n))


"""
4. dataframe of top hashtags
"""
def top_n_items_df(items, n, key_name):
    """
    :return: a dataframe containing n most-frequent items from a list
    """

    FREQ_STR = 'freq'

    df = pd.DataFrame(items, columns=[key_name])
    return df.groupby(key_name).size().reset_index().rename({0: FREQ_STR}, axis='columns').\
        sort_values(FREQ_STR, ascending=False).head(n)

print('\nDataframe of top %d tweeted hashtags of tweets1.json:' % n)
top_df_1 = top_n_items_df(hashtags_tweets1, n, HASHTAG_STR)
print(top_df_1)

print('\nDataframe of top %d tweeted hashtags of tweets2.json:' % n)
top_df_2 = top_n_items_df(hashtags_tweets2, n, HASHTAG_STR)
print(top_df_2)


"""
5. horizontal bar charts
"""
ax = top_df_1.plot(kind='barh', x=HASHTAG_STR, y='freq',
                   title = 'hashtags count before presidential debate', color='blue')
ax.figure.gca().invert_yaxis()
ax.figure.savefig(path.join(outputdir, 'hbar_tweets1.png'))
plt.show()

ax = top_df_2.plot(kind='barh', x=HASHTAG_STR, y='freq',
                   title = 'hashtags count after presidential debate', color='blue')
ax.figure.gca().invert_yaxis()
ax.figure.savefig(path.join(outputdir, 'hbar_tweets2.png'))
plt.show()



"""
6. max and min times
"""

def microseconds_to_ts(microseconds):
    return datetime.fromtimestamp(microseconds / 1000)

def get_min_max_times(tweets):
    """
    :return: min datetime stamp, max datetime stamp
    """

    temp = [int(t[TIMESTAMP_STR]) for t in tweets]
    return min(temp), max(temp)


min_ms_1, max_ms_1 = get_min_max_times(clean_tweets1)
print('min and max timestamps for tweets1.json:')
print(microseconds_to_ts(min_ms_1), microseconds_to_ts(max_ms_1))

min_ms_2, max_ms_2 = get_min_max_times(clean_tweets2)
print('min and max timestamps for tweets2.json:')
print(microseconds_to_ts(min_ms_2), microseconds_to_ts(max_ms_2))


"""
7. divide [min ts, max ts] into 10 equally spaced periods 
"""

def get_time_intervals(min_time, max_time, n):
    """
    :param min_time: lower bound in microseconds
    :param max_time: upper bound in microseconds
    :param n: number of intervals (will return n+1 points)
    :return: as list of points in microseconds
    """
    res = np.linspace(start=min_time, stop=max_time, num=n+1)

    for div in res:
        print(microseconds_to_ts(div))

    return res

print('Intervals for tweets1.json:')
timebins_1 = get_time_intervals(min_ms_1, max_ms_1, 10)

print('Intervals for tweets2.json:')
timebins_2 = get_time_intervals(min_ms_2, max_ms_2, 10)


"""
8. hashtag | creation | interval
"""

def get_hashtag_creation_bin_df(tweets, timebins):

    hashtag_creations = []  # [(tag, timestamp), ...]

    for tweet in tweets:
        ts = int(tweet[TIMESTAMP_STR])
        hashtag_creations += [(tag, ts) for tag in get_hashtags(tweet, does_include_retweet=False)]

        # todo: not including retweets because their timestamps may not fit into
        #       the post-creation time intervals from the previous step

    df = pd.DataFrame(hashtag_creations, columns=[HASHTAG_STR, CREATED_STR])
    df[LEVEL_STR] = pd.cut(df[CREATED_STR], timebins, include_lowest=True)

    # sanity check
    assert(df[LEVEL_STR].isna().sum()==0)

    return df

hashtag_df_1 = get_hashtag_creation_bin_df(clean_tweets1, timebins_1)
hashtag_df_2 = get_hashtag_creation_bin_df(clean_tweets2, timebins_2)

print('\nSnippet of hashtag creation for tweets1.json:')
print(hashtag_df_1.head())
print('\nSnippet of hashtag creation for tweets2.json:')
print(hashtag_df_2.head())


"""
9. row: timeperiod; column: hashtag; value: number of occurrences
"""
def get_hashtag_created_pivot(hashtag_creation_df):
    res = hashtag_creation_df.pivot_table(index=LEVEL_STR, columns=HASHTAG_STR,
                                           aggfunc=len, fill_value=0)

    # drop redundant multiindex column
    res.columns = res.columns.droplevel(0)

    # sanity check
    assert(len(res.columns) == len(hashtag_creation_df[HASHTAG_STR].unique()))

    return res

hashtag_pivot_1 = get_hashtag_created_pivot(hashtag_df_1)
hashtag_pivot_2 = get_hashtag_created_pivot(hashtag_df_2)

print('\nSnippet of hashtag creation pivot for tweets1.json:')
print(hashtag_pivot_1.head())
print('\nSnippet of hashtag creation pivot for tweets2.json:')
print(hashtag_pivot_2.head())


"""
10. number of occurences of specific tags
"""

# What is the number of occurrence of hashtag 'trump' in the sixth period in the
# tweets1.json?
print('The number of occurrence of hashtag \'trump\' in the sixth period in the '
      'tweets1.json is %d.' %
      hashtag_pivot_1['trump'][hashtag_pivot_1.index.categories[6]])


# What is the number of occurrence of hashtag 'trump' in the eighth period in
# the tweets2.json?
print('The number of occurrence of hashtag \'trump\' in the eighth period in the '
      'tweets2.json is %d.' %
      hashtag_pivot_2['trump'][hashtag_pivot_2.index.categories[8]])


"""
11. plot hashtag occurrence over time
"""

def plot_hashtag_occ(pivottable, n, plot_title, plot_filename):

    top_tags = list(pivottable.sum().sort_values(ascending=False)[:n].index)

    pivottable[top_tags].plot(subplots=True,
                              legend=False, layout=(5, 4), figsize=(5*2, 4*2),
                              title=top_tags,
                              grid=True)

    plt.subplots_adjust(wspace=0.4, hspace=0.5)
    plt.suptitle(plot_title)
    plt.savefig(path.join(outputdir, plot_filename))
    plt.show()

n = 20
plot_hashtag_occ(hashtag_pivot_1, n,
                 'Hashtags occurrence over time for tweets1.json',
                 'hashtag_subplots_tweets1.png')
plot_hashtag_occ(hashtag_pivot_2, n,
                 'Hashtags occurrence over time for tweets2.json',
                 'hashtag_subplots_tweets2.png')

"""
12. read zip_codes_states.csv
"""
zipstate_data = pd.read_csv(path.join(inputdir, 'zip_codes_states.csv'))


"""
13. select tweets only in zip_codes_states.csv; not london
"""
us_places = set(zipstate_data[CITY_STR].values).union(set(zipstate_data[COUNTY_STR].values))
if np.nan in us_places:
    us_places.remove(np.nan)
us_places = set([v.lower() for v in us_places])

def get_us_tweets(tweets):
    res = []

    for tweet in tweets:
        loc, is_loc_legit = is_location_legit(tweet, us_places)
        if is_loc_legit and loc.lower() != 'london':
            res.append(tweet)

    return res

us_tweets_1 = get_us_tweets(clean_tweets1)
us_tweets_2 = get_us_tweets(clean_tweets2)

print('%d out of %d clean tweets from tweets1.json are from the US.' % (len(us_tweets_1), len(clean_tweets1)))
print('%d out of %d clean tweets from tweets2.json are from the US.' % (len(us_tweets_2), len(clean_tweets2)))


"""
14. top 20 locations
"""
n = 20
print('Top %d locations from tweets1.json:' % n)
top_n_items([get_location(t) for t in us_tweets_1], n)

print('Top %d locations from tweets2.json:' % n)
top_n_items([get_location(t) for t in us_tweets_2], n)


"""
15. average lat long for every location
"""
grouped_zipdf = zipstate_data.groupby([CITY_STR, STATE_STR, COUNTY_STR], as_index=False).mean()
del grouped_zipdf['zip_code']


"""
16. 1 + 2: location | count | lat | long
"""

# combine 1 and 2
alltweets = us_tweets_1 + us_tweets_2

# create standardized us location mapping
std_location_dict = {}      # {location token: row number}
location_names = {CITY_STR: set(), COUNTY_STR: set()}

for i, row in grouped_zipdf.iterrows():
    for k in [CITY_STR, COUNTY_STR]:
        key = row[k].lower()
        location_names[k].add(key)

        std_location_dict[key] = std_location_dict.get(key, []) + [i]

# create data frames which contain locations, counts, longitude and latitude
tuples = []
for tweet in alltweets:
    location = get_location(tweet)

    if not location:
        continue

    location_tokens = set([t.strip() for t in location.lower().split(',')])
    for token in location_tokens:
        if token in std_location_dict:
            num_hashtags = len(get_hashtags(tweet, does_include_retweet=True))

            zipdf_row = grouped_zipdf.iloc[std_location_dict[token][0]]   # This isn't correct. But again the instrumentation wasn't correct to begin with.

            tuples.append((zipdf_row[CITY_STR], zipdf_row[STATE_STR], zipdf_row[COUNTY_STR],
                           zipdf_row['latitude'], zipdf_row['longitude'],
                           num_hashtags))

            continue

location_df = pd.DataFrame(tuples,
                           columns=[CITY_STR, STATE_STR, COUNTY_STR, 'latitude', 'longitude', 'num_hashtags'])


"""
17. plot number of hashtags on the us map
"""

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from matplotlib.colors import rgb2hex, Normalize
from matplotlib.patches import Polygon
from matplotlib.colorbar import ColorbarBase

fig, ax = plt.subplots()

# Lambert Conformal map of lower 48 states.
m = Basemap(llcrnrlon=-119,llcrnrlat=20,urcrnrlon=-64,urcrnrlat=49,
            projection='lcc',lat_1=33,lat_2=45,lon_0=-95)

# draw state boundaries
shp_info = m.readshapefile(path.join(inputdir, 'st99_d00'),
                           'states',
                           drawbounds=True,
                           linewidth=0.45,
                           color='gray')
