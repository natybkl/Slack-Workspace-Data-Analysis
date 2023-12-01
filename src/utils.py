import os
import re
import sys
import glob
import json
import datetime
import nltk

import warnings
warnings.filterwarnings('ignore')
import matplotlib.pyplot as plt
import seaborn as sns
import gensim
from gensim.models import CoherenceModel
from gensim import corpora
import pandas as pd
from pprint import pprint
import string

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer
import pyLDAvis.gensim_models as gensimvis
import pyLDAvis

from collections import Counter


import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
from nltk.corpus import stopwords


def break_combined_weeks(combined_weeks):
    """
    Breaks combined weeks into separate weeks.
    
    Args:
        combined_weeks: list of tuples of weeks to combine
        
    Returns:
        tuple of lists of weeks to be treated as plus one and minus one
    """
    plus_one_week = []
    minus_one_week = []

    for week in combined_weeks:
        if week[0] < week[1]:
            plus_one_week.append(week[0])
            minus_one_week.append(week[1])
        else:
            minus_one_week.append(week[0])
            plus_one_week.append(week[1])

    return plus_one_week, minus_one_week

def get_msgs_df_info(df):
    msgs_count_dict = df.user.value_counts().to_dict()
    replies_count_dict = dict(Counter([u for r in df.replies if r != None for u in r]))
    mentions_count_dict = dict(Counter([u for m in df.mentions if m != None for u in m]))
    links_count_dict = df.groupby("user").link_count.sum().to_dict()
    return msgs_count_dict, replies_count_dict, mentions_count_dict, links_count_dict



def get_messages_dict(msgs):
    msg_list = {
            "msg_id":[],
            "text":[],
            "attachments":[],
            "user":[],
            "mentions":[],
            "emojis":[],
            "reactions":[],
            "replies":[],
            "replies_to":[],
            "ts":[],
            "links":[],
            "link_count":[]
            }


    for msg in msgs:
        if "subtype" not in msg:
            try:
                msg_list["msg_id"].append(msg["client_msg_id"])
            except:
                msg_list["msg_id"].append(None)
            
            msg_list["text"].append(msg["text"])
            msg_list["user"].append(msg["user"])
            msg_list["ts"].append(msg["ts"])
            
            if "reactions" in msg:
                msg_list["reactions"].append(msg["reactions"])
            else:
                msg_list["reactions"].append(None)

            if "parent_user_id" in msg:
                msg_list["replies_to"].append(msg["ts"])
            else:
                msg_list["replies_to"].append(None)

            if "thread_ts" in msg and "reply_users" in msg:
                msg_list["replies"].append(msg["replies"])
            else:
                msg_list["replies"].append(None)
            
            if "blocks" in msg:
                emoji_list = []
                mention_list = []
                link_count = 0
                links = []
                
                for blk in msg["blocks"]:
                    if "elements" in blk:
                        for elm in blk["elements"]:
                            if "elements" in elm:
                                for elm_ in elm["elements"]:
                                    
                                    if "type" in elm_:
                                        if elm_["type"] == "emoji":
                                            emoji_list.append(elm_["name"])

                                        if elm_["type"] == "user":
                                            mention_list.append(elm_["user_id"])
                                        
                                        if elm_["type"] == "link":
                                            link_count += 1
                                            links.append(elm_["url"])


                msg_list["emojis"].append(emoji_list)
                msg_list["mentions"].append(mention_list)
                msg_list["links"].append(links)
                msg_list["link_count"].append(link_count)
            else:
                msg_list["emojis"].append(None)
                msg_list["mentions"].append(None)
                msg_list["links"].append(None)
                msg_list["link_count"].append(0)
    
    return msg_list

def from_msg_get_replies(msg):
    replies = []
    if "thread_ts" in msg and "replies" in msg:
        try:
            for reply in msg["replies"]:
                reply["thread_ts"] = msg["thread_ts"]
                reply["message_id"] = msg["client_msg_id"]
                replies.append(reply)
        except:
            pass
    return replies

def msgs_to_df(msgs):
    msg_list = get_messages_dict(msgs)
    df = pd.DataFrame(msg_list)
    return df

def process_msgs(msg):
    '''
    select important columns from the message
    '''

    keys = ["client_msg_id", "type", "text", "user", "ts", "team", 
            "thread_ts", "reply_count", "reply_users_count", "replies_to"]
    
    msg_list = {k:msg[k] for k in keys}
    rply_list = from_msg_get_replies(msg)

    return msg_list, rply_list


def get_messages_from_channel(channel_path):
    '''
    get all the messages from a channel        
    '''
    json_files = [
        f"{channel_path}/{pos_json}" 
        for pos_json in os.listdir(channel_path) 
        if pos_json.endswith('.json')
    ]    
    combined = []

    for json_file in json_files:
        with open(json_file, 'r', encoding="utf8") as slack_data:
            json_content = json.load(slack_data)
            combined.extend(json_content)
        
    msg_list = get_messages_dict(combined)
    df = pd.DataFrame(msg_list)
    
    return df

def get_all_channels_messages(channels):
    messages = []
    for channel in channels:
        # print(channel)
        base_path = "../anonymized/" + channel['name'] + '/'

        json_files = [f"{base_path}/{pos_json}"  for pos_json in os.listdir(base_path) if pos_json.endswith('.json')]  

        combined = []

        for json_file in json_files:
            with open(json_file, 'r', encoding="utf8") as slack_data:
                json_content = json.load(slack_data)
            combined.extend(json_content)
        
        for msg in combined:
            if "subtype" not in msg:
                text = msg.get("text", None)
                ts = msg.get("ts", None)

                messages.append((text, ts))
        
    return messages

def get_all_channels_replies(channels):
    replies = []
    for channel in channels:
        # print(channel)
        base_path = "../anonymized/" + channel['name'] + '/'

        json_files = [f"{base_path}/{pos_json}"  for pos_json in os.listdir(base_path) if pos_json.endswith('.json')]  

        combined = []

        for json_file in json_files:
            with open(json_file, 'r', encoding="utf8") as slack_data:
                json_content = json.load(slack_data)
            combined.extend(json_content)

        reply_timestamps = []

        for msg in combined:    
            msg_reply = from_msg_get_replies(msg) 
            if msg_reply: 
                reply_timestamps.append(msg_reply)

        replies.extend(reply_timestamps)

    return replies

def convert_2_timestamp(column, data):
    """convert from unix time to readable timestamp
        args: column: columns that needs to be converted to timestamp
                data: data that has the specified column
    """
    if column in data.columns.values:
        timestamp_ = []
        for time_unix in data[column]:
            if time_unix == 0:
                timestamp_.append(0)
            else:
                a = datetime.datetime.fromtimestamp(float(time_unix))
                timestamp_.append(a.strftime('%Y-%m-%d %H:%M:%S'))
        return timestamp_
    else: print(f"{column} not in data")


def get_tagged_users(df):
    """get all @ in the messages"""

    return df['msg_content'].map(lambda x: re.findall(r'@U\w+', x))


def map_userid_2_realname(user_profile: dict, comm_dict: dict, plot=False):
    """
    map slack_id to realnames
    user_profile: a dictionary that contains users info such as real_names
    comm_dict: a dictionary that contains slack_id and total_message sent by that slack_id
    """
    user_dict = {} # to store the id
    real_name = [] # to store the real name
    ac_comm_dict = {} # to store the mapping
    count = 0
    # collect all the real names
    for i in range(len(user_profile['profile'])):
        real_name.append(dict(user_profile['profile'])[i]['real_name'])

    # loop the slack ids
    for i in user_profile['id']:
        user_dict[i] = real_name[count]
        count += 1

    # to store mapping
    for i in comm_dict:
        if i in user_dict:
            ac_comm_dict[user_dict[i]] = comm_dict[i]

    ac_comm_dict = pd.DataFrame(data= zip(ac_comm_dict.keys(), ac_comm_dict.values()),
    columns=['LearnerName', '# of Msg sent in Threads']).sort_values(by='# of Msg sent in Threads', ascending=False)
    
    if plot:
        ac_comm_dict.plot.bar(figsize=(15, 7.5), x='LearnerName', y='# of Msg sent in Threads')
        plt.title('Student based on Message sent in thread', size=20)
        
    return ac_comm_dict

def get_community_participation(path):
    """ specify path to get json files"""
    combined = []
    comm_dict = {}
    for json_file in glob.glob(f"{path}*.json"):
        with open(json_file, 'r') as slack_data:
            combined.append(slack_data)
    # print(f"Total json files is {len(combined)}")

    for i in combined:
        a = json.load(open(i.name, 'r', encoding='utf-8'))

        for msg in a:
            if 'replies' in msg.keys():
                for i in msg['replies']:
                    comm_dict[i['user']] = comm_dict.get(i['user'], 0)+1

    return comm_dict


def preprocess_text(text):

    # print("here")
    # Extract and remove URLs
    urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
    for url in urls:
        text = text.replace(url, '')

    text = re.sub(r'<@.*?>', '', text)

    # Convert to lowercase
    text = text.lower()

    # Remove punctuation
    text = ''.join([char for char in text if char not in string.punctuation])

    # Remove numbers
    text = re.sub(r'\d+', '', text)

    # Tokenize
    tokens = word_tokenize(text)

    # Remove stop words
    stop_words = set(stopwords.words('english'))
    tokens = [word for word in tokens if word not in stop_words]

    # Perform stemming
    stemmer = PorterStemmer()
    tokens = [stemmer.stem(word) for word in tokens]

    # Perform lemmatization
    lemmatizer = WordNetLemmatizer()
    tokens = [lemmatizer.lemmatize(word) for word in tokens]

    # Join the tokens back into a string
    text = ' '.join(tokens)

    return text
