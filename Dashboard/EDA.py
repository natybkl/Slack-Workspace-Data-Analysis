
# %%


# %%
import os, sys
import re
import json
import glob
import datetime
from collections import Counter

import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

from nltk.corpus import stopwords
from wordcloud import WordCloud


# %% [markdown]
# ### Columns we can get from a slack message<br>
# 
# message_type, message_content, sender_id, time_sent, message_distribution, time_thread_start, reply_count, reply_user_count, time_thread_end, reply_users

# %% [markdown]
# From a single slack message, we can get <br>
# 
# 1. The message<br>
# 2. Type (message, file, link, etc)<br>
# 3. The sender_id (assigned by slack)<br>
# 4. The time the message was sent<br>
# 5. The team (i don't know what that is now)<br>
# 6. The type of the message (broadcast message, inhouse, just messgae)<br>
# 7. The thread the message generated (from here we can go):<br>
#     7.1 Text/content of the message<br>
#     7.2 The thread time of the message<br>
#     7.3 The thread count (reply count)<br>
#     7.4 The number of user that reply the message (count of users that participated in the thread)<br>
#     7.5 The time the last thread message was sent <br>
#     7.6 The users that participated in the thread (their ids are stored as well)<br>

# %%
def get_top_20_user(data, channel='Random'):
    """get user with the highest number of message sent to any channel"""

    data['sender_name'].value_counts()[:20].plot.bar(figsize=(15, 7.5))
    plt.title(f'Top 20 Message Senders in #{channel} channels', size=15, fontweight='bold')
    plt.xlabel("Sender Name", size=18); plt.ylabel("Frequency", size=14);
    plt.xticks(size=12); plt.yticks(size=12);
    plt.show()

    data['sender_name'].value_counts()[-10:].plot.bar(figsize=(15, 7.5))
    plt.title(f'Bottom 10 Message Senders in #{channel} channels', size=15, fontweight='bold')
    plt.xlabel("Sender Name", size=18); plt.ylabel("Frequency", size=14);
    plt.xticks(size=12); plt.yticks(size=12);
    plt.show()

def draw_avg_reply_count(data, channel='Random'):
    """who commands many reply?"""

    data.groupby('sender_name')['reply_count'].mean().sort_values(ascending=False)[:20]\
        .plot(kind='bar', figsize=(15,7.5));
    plt.title(f'Average Number of reply count per Sender in #{channel}', size=20, fontweight='bold')
    plt.xlabel("Sender Name", size=18); plt.ylabel("Frequency", size=18);
    plt.xticks(size=14); plt.yticks(size=14);
    plt.show()

def draw_avg_reply_users_count(data, channel='Random'):
    """who commands many user reply?"""

    data.groupby('sender_name')['reply_users_count'].mean().sort_values(ascending=False)[:20].plot(kind='bar',
     figsize=(15,7.5));
    plt.title(f'Average Number of reply user count per Sender in #{channel}', size=20, fontweight='bold')
    plt.xlabel("Sender Name", size=18); plt.ylabel("Frequency", size=18);
    plt.xticks(size=14); plt.yticks(size=14);
    plt.show()

def draw_wordcloud(msg_content, week):    
    # word cloud visualization
    allWords = ' '.join([twts for twts in msg_content])
    wordCloud = WordCloud(background_color='#975429', width=500, height=300, random_state=21, max_words=500, mode='RGBA',
                            max_font_size=140, stopwords=stopwords.words('english')).generate(allWords)
    plt.figure(figsize=(15, 7.5))
    plt.imshow(wordCloud, interpolation="bilinear")
    plt.axis('off')
    plt.tight_layout()
    plt.title(f'WordCloud for {week}', size=30)
    plt.show()

def draw_user_reaction(data, channel='General'):
    data.groupby('sender_name')[['reply_count', 'reply_users_count']].sum()\
        .sort_values(by='reply_count',ascending=False)[:10].plot(kind='bar', figsize=(15, 7.5))
    plt.title(f'User with the most reaction in #{channel}', size=25);
    plt.xlabel("Sender Name", size=18); plt.ylabel("Frequency", size=18);
    plt.xticks(size=14); plt.yticks(size=14);
    plt.show()

# %%
rpath = os.path.abspath('..')
if rpath not in sys.path:
    sys.path.insert(0, rpath)

import os
import pandas as pd
import glob
from src.loader import SlackDataLoader, slack_parser, parse_slack_reaction
from src.utils import get_messages_dict

# Provide the path to the Slack exported data folder
slack_data_path = os.path.abspath('../anonymized')

# Create an instance of SlackDataLoader
slack_data_loader = SlackDataLoader(slack_data_path)

# Get the users and channels from the Slack data
users = slack_data_loader.get_users()
channels = slack_data_loader.get_channels()
channel_names = []
user_names = []

for channel in channels:
    channel_names.append(channel['name'])

for user in users:
    user_names.append(user['name'])

# print(channel_names)


# %%
parsed = slack_parser('../anonymized/all-community-building/')

parsed['channel']

# %%
def create_combined_dataframe(channel_names):
    data_frames = []
    ROOT_DIR = '../anonymized/'

    for channel in channel_names:
        channel_path = ROOT_DIR + channel +  '/'
        channel_dataframe = slack_parser(channel_path)
        data_frames.append(channel_dataframe)
        

    combined_data = pd.concat(data_frames, ignore_index=True)

    return combined_data

combined_data = create_combined_dataframe(channel_names)  


# %% [markdown]
# ## Insight Extraction
# 
# Below are some useful questions to answer. Feel free to explore to answer other interesting questions that may be of help to get insight about student's behaviour, need, and future performance 

# %%
def get_top_10_users_by_reply(df):
     # Get value counts of 'sender_id' column
    user_reply_counts = df['sender_name'].value_counts()

    # Get the top 10 users by reply count
    top_10_users = user_reply_counts.head(10)

    return top_10_users

def get_top_repliers():
    return get_bottom_10_users_by_reply(combined_data).reset_index()

def get_bottom_10_users_by_reply(df):
    # Get value counts of 'sender_id' column
    user_reply_counts = df['sender_name'].value_counts()

    # Get the bottom 10 users by reply count
    bottom_10_users = user_reply_counts.tail(10)

    return bottom_10_users

# %%
get_top_10_users_by_reply(combined_data)

# %%
get_bottom_10_users_by_reply(combined_data)

# %%
def get_top_users_by_message_count(df):
    # Group the DataFrame by 'sender_name' and calculate the count of unique messages for each user
    user_message_counts = df.groupby('sender_name')['msg_content'].nunique()

    # Sort the user_message_counts Series in descending order
    sorted_user_message_counts = user_message_counts.sort_values(ascending=False)

    # Get the top users by message count
    top_users = sorted_user_message_counts.head(10)

    return top_users

def get_bottom_users_by_message_count(df):
    # Group the DataFrame by 'sender_name' and calculate the count of unique messages for each user
    user_message_counts = df.groupby('sender_name')['msg_content'].nunique()

    # Sort the user_message_counts Series in ascending order
    sorted_user_message_counts = user_message_counts.sort_values(ascending=True)

    # Get the bottom users by message count
    bottom_users = sorted_user_message_counts.head(10)

    return bottom_users

# %%
get_top_users_by_message_count(combined_data)

# %%
get_bottom_users_by_message_count(combined_data)

# %%
def get_top_10_messages_by_replies(df):
    # Sort the DataFrame by 'reply_count' column in descending order
    sorted_df = df.sort_values('reply_count', ascending=False)

    # Get the top 10 messages by replies
    top_10_messages = sorted_df.head(10)

    return top_10_messages

# %%
get_top_10_messages_by_replies(combined_data)


# %%
#plot of highest number of reply counts per user
def plot_highest_replies_per_user(data):
    grouped_df = data.groupby('sender_name')['reply_count'].sum()
    grouped_df = grouped_df.sort_values(ascending=False)
    grouped_df.plot(kind='bar', figsize=(15, 7.5))
    
    plt.title('Reply Counts of users')
    plt.xlabel('User')
    plt.ylabel('Number of replies')
    plt.show()

plot_highest_replies_per_user(combined_data)

# %%
# Visualize reply counts per user per channel
def reply_per_user_per_channel(data):
    grouped_df = data.groupby(['channel', 'sender_name'])['reply_count'].sum().unstack()
    grouped_df.plot(kind='bar', figsize=(15, 7.5), stacked=True)
    
    plt.title('Reply Counts per User per Channel')
    plt.xlabel('Channel')
    plt.ylabel('Total Reply Count')
    plt.legend(title='Sender Name', bbox_to_anchor=(1, 1))
    plt.show()

    
reply_per_user_per_channel(combined_data)

# %%
get_top_20_user(combined_data)

# %%
# which user has the highest number of reply counts?

# %%
# Visualize reply counts per user per channel

# %%
# what is the time range of the day that most messages are sent?


# %%
# what kind of messages are replied faster than others?

# %%
# Relationship between # of messages and # of reactions

# %%
# Classify messages into different categories such as questions, answers, comments, etc.

# %%
# Which users got the most reactions?

# %%
# Model topics mentioned in the channel

# %%
# What are the topics that got the most reactions?

# %% [markdown]
# ### Harder questions to look into

# %%
# Based on messages, reactions, references shared, and other relevant data such as classification of questions into techical question, comment, answer, aorder stu the python, statistics, and sql skill level of a user?


