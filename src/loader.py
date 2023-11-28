import json
import fnmatch
import argparse
import os
import pandas as pd
import glob
import io
import shutil
import copy
from datetime import datetime
from pick import pick
from time import sleep


# Create wrapper classes for using slack_sdk in place of slacker
class SlackDataLoader:
    '''
    Slack exported data IO class.

    When you open slack exported ZIP file, each channel or direct message 
    will have its own folder. Each folder will contain messages from the 
    conversation, organised by date in separate JSON files.

    You'll see reference files for different kinds of conversations: 
    users.json files for all types of users that exist in the slack workspace
    channels.json files for public channels, 
    
    These files contain metadata about the conversations, including their names and IDs.

    For secruity reason, we have annonymized names - the names you will see are generated using faker library.
    
    '''
    def __init__(self, path):
        '''
        path: path to the slack exported data folder
        '''
        self.path = path
        self.channels = self.get_channels()
        self.users = self.get_users()
    

    def get_users(self):
        '''
        write a function to get all the users from the json file
        '''
        with open(os.path.join(self.path, 'users.json'), 'r') as f:
            users = json.load(f)

        return users
    
    def get_channels(self):
        '''
        write a function to get all the channels from the json file
        '''
        with open(os.path.join(self.path, 'channels.json'), 'r') as f:
            channels = json.load(f)

        return channels

    def get_channel_messages(self, channel_name):
        '''
        write a function to get all the messages from a channel
        
        '''

    # 
    def get_user_map(self):
        '''
        write a function to get a map between user id and user name
        '''
        userNamesById = {}
        userIdsByName = {}
        for user in self.users:
            userNamesById[user['id']] = user['name']
            userIdsByName[user['name']] = user['id']
        return userNamesById, userIdsByName    


# combine all json file in all-weeks8-9
# def slack_parser(path_channel):

#     # specify path to get json files
#     combined = []
#     for json_file in glob.glob(f"{path_channel}*.json"):
#         with open(json_file, 'r', encoding="utf8") as slack_data:
#             combined.append(slack_data)

#     # loop through all json files and extract required informations
#     dflist = []
#     for slack_data in combined:

#         msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st, reply_users, \
#         reply_count, reply_users_count, tm_thread_end = [],[],[],[],[],[],[],[],[],[]

#         for row in slack_data:
#             if 'bot_id' in row.keys():
#                 continue
#             else:
#                 msg_type.append(row['type'])
#                 msg_content.append(row['text'])
#                 if 'user_profile' in row.keys(): sender_id.append(row['user_profile']['real_name'])
#                 else: sender_id.append('Not provided')
#                 time_msg.append(row['ts'])
#                 if 'blocks' in row.keys() and len(row['blocks'][0]['elements'][0]['elements']) != 0 :
#                     msg_dist.append(row['blocks'][0]['elements'][0]['elements'][0]['type'])
#                 else: msg_dist.append('reshared')
#                 if 'thread_ts' in row.keys():
#                     time_thread_st.append(row['thread_ts'])
#                 else:
#                     time_thread_st.append(0)
#                 if 'reply_users' in row.keys(): reply_users.append(",".join(row['reply_users'])) 
#                 else:    reply_users.append(0)
#                 if 'reply_count' in row.keys():
#                     reply_count.append(row['reply_count'])
#                     reply_users_count.append(row['reply_users_count'])
#                     tm_thread_end.append(row['latest_reply'])
#                 else:
#                     reply_count.append(0)
#                     reply_users_count.append(0)
#                     tm_thread_end.append(0)
#         data = zip(msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st,
#         reply_count, reply_users_count, reply_users, tm_thread_end)
#         columns = ['msg_type', 'msg_content', 'sender_name', 'msg_sent_time', 'msg_dist_type',
#         'time_thread_start', 'reply_count', 'reply_users_count', 'reply_users', 'tm_thread_end']

#         df = pd.DataFrame(data=data, columns=columns)
#         df = df[df['sender_name'] != 'Not provided']
#         dflist.append(df)

#     dfall = pd.concat(dflist, ignore_index=True)
#     dfall['channel'] = path_channel.split('/')[-1].split('.')[0]        
#     dfall = dfall.reset_index(drop=True)
    
#     return dfall


def parse_slack_reaction(path, channel):
    """get reactions"""
    dfall_reaction = pd.DataFrame()
    combined = []

    for json_file in glob.glob(f"{path}*.json"):
        with open(json_file, 'r') as slack_data:
            combined.append(slack_data)

    reaction_name, reaction_count, reaction_users, msg, user_id = [], [], [], [], []

    for k in combined:
        slack_data = json.load(open(k.name, 'r', encoding="utf-8"))
        
        for i_count, i in enumerate(slack_data):
            if 'reactions' in i.keys():
                for j in range(len(i['reactions'])):
                    msg.append(i['text'])
                    user_id.append(i['user'])
                    reaction_name.append(i['reactions'][j]['name'])
                    reaction_count.append(i['reactions'][j]['count'])
                    reaction_users.append(",".join(i['reactions'][j]['users']))
                
    data_reaction = zip(reaction_name, reaction_count, reaction_users, msg, user_id)
    columns_reaction = ['reaction_name', 'reaction_count', 'reaction_users_count', 'message', 'user_id']
    df_reaction = pd.DataFrame(data=data_reaction, columns=columns_reaction)
    df_reaction['channel'] = channel
    return df_reaction

def slack_parser(path_channel):

    """ parse slack data to extract useful informations from the json file
        step of execution
        1. Import the required modules
        2. read all json file from the provided path
        3. combine all json files in the provided path
        4. extract all required informations from the slack data
        5. convert to dataframe and merge all
        6. reset the index and return dataframe
    """
     
    # file_list = os.listdir(path_channel)
    # json_files = [file for file in file_list if file.endswith('.json')]
    # print(json_files)

    combined = []


    json_files = []
    for file in os.listdir(path_channel):
        if fnmatch.fnmatch(file, '*.json') and os.path.isfile(os.path.join(path_channel, file)):
            json_files.append(file)

    for json_file in json_files:
        with open(os.path.join(path_channel, json_file), 'r', encoding="utf8") as slack_data:
            content = slack_data.read()
            messages = json.loads(content)
            combined.append(messages)

    
    # for json_file in glob.glob(f"{path_channel}*.json"):
    #     with open(json_file, 'r', encoding="utf8") as slack_data:
    #         combined.append(slack_data)

  
    dflist = []
    for slack_data in combined:
        for message in slack_data:
            # print(type(message))
            if 'bot_id' in message.keys():
                continue
            
            print(message)
            msg_type = message.get('type', 'Not provided')
            msg_content = message.get('text', 'Not provided')

            sender_name = 'Not provided'
            if 'user_profile' in message.keys() and 'real_name' in message['user_profile'].keys():
                sender_name = message['user_profile']['real_name']

            msg_sent_time = message.get('ts', 'Not provided')
            msg_dist_type = 'reshared'
            if 'blocks' in message.keys() and len(message['blocks']) > 0:
                block = message['blocks'][0]
                if 'elements' in block.keys() and len(block['elements']) > 0:
                    element = block['elements'][0]
                    if 'elements' in element.keys() and len(element['elements']) > 0:
                        msg_dist_type = element['elements'][0].get('type', 'reshared')

            time_thread_start = message.get('thread_ts', 0)
            reply_count = message.get('reply_count', 0)
            reply_users_count = message.get('reply_users_count', 0)
            reply_users = ",".join(message.get('reply_users', []))
            tm_thread_end = message.get('latest_reply', 0)

            data = [
                (msg_type, msg_content, sender_name, msg_sent_time, msg_dist_type,
                 time_thread_start, reply_count, reply_users_count, reply_users, tm_thread_end)
            ]

            columns = ['msg_type', 'msg_content', 'sender_name', 'msg_sent_time', 'msg_dist_type',
                       'time_thread_start', 'reply_count', 'reply_users_count', 'reply_users', 'tm_thread_end']
            
            df = pd.DataFrame(data=data, columns=columns)

            dflist.append(df)


    # print("Here is the Deal")

    if len(dflist) > 0:
        dfall = pd.concat(dflist, ignore_index=True)
        dfall = dfall[dfall['sender_name'] != 'Not provided']
        dfall['channel'] = path_channel.split('/')[-1].split('.')[0]
        dfall = dfall.reset_index(drop=True)
        return dfall
    else:
        # print('Here is the deal')
        return pd.DataFrame()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export Slack history')

    parser.add_argument('--zip', help="Name of a zip file to import")
    args = parser.parse_args()
