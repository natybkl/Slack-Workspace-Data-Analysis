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


def extract_messages_from_directory(directory):
    messages = []

    # Get the absolute path of the directory
    directory = os.path.abspath(directory)

    # Iterate through the directory and its subdirectories
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)

                # Read the JSON file
                with open(file_path, 'r') as f:
                    try:
                        json_data = json.load(f)

                        if json_data:
                            # Extract fields from the JSON data
                            extracted_data = extract_fields(json_data)
                            messages.append(extracted_data)
                        else:
                            print(f"Empty JSON data in file: {file_path}")

                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON in file {file_path}: {e}")

    return messages

def extract_fields(json_data):
    # Extract specific fields from the JSON data
    extracted_data = {
        'client_msg_id': json_data.get('client_msg_id', ''),
        'type': json_data.get('type', ''),
        'text': json_data.get('text', ''),
        'user': json_data.get('user', ''),
        'ts': json_data.get('ts', ''),
        'team': json_data.get('team', ''),
        'user_team': json_data.get('user_team', ''),
        'source_team': json_data.get('source_team', ''),
        'user_profile': extract_user_profile(json_data.get('user_profile', {})),
        'attachments': extract_attachments(json_data.get('attachments', [])),
        'blocks': extract_blocks(json_data.get('blocks', [])),
        'thread_ts': json_data.get('thread_ts', ''),
        'reply_count': json_data.get('reply_count', 0),
        'reply_users_count': json_data.get('reply_users_count', 0),
        'latest_reply': json_data.get('latest_reply', ''),
        'reply_users': json_data.get('reply_users', []),
        'replies': json_data.get('replies', []),
        'is_locked': json_data.get('is_locked', False),
        'subscribed': json_data.get('subscribed', False),
        'reactions': json_data.get('reactions', [])
    }

    if not all(extracted_data.values()):
        print(f"Empty or NoneType data found: {json_data}")

    return extracted_data

def extract_user_profile(user_profile):
    # Extract specific fields from the user profile
    extracted_profile = {
        'avatar_hash': user_profile.get('avatar_hash', ''),
        'image_72': user_profile.get('image_72', ''),
        'first_name': user_profile.get('first_name', ''),
        'real_name': user_profile.get('real_name', ''),
        'display_name': user_profile.get('display_name', ''),
        'team': user_profile.get('team', ''),
        'name': user_profile.get('name', ''),
        'is_restricted': user_profile.get('is_restricted', False),
        'is_ultra_restricted': user_profile.get('is_ultra_restricted', False)
    }

    return extracted_profile

def extract_attachments(attachments):
    extracted_attachments = []

    for attachment in attachments:
        # Extract specific fields from the attachment
        extracted_attachment = {
            'from_url': attachment.get('from_url', ''),
            'image_url': attachment.get('image_url', ''),
            'image_width': attachment.get('image_width', 0),
            'image_height': attachment.get('image_height', 0),
            'image_bytes': attachment.get('image_bytes', 0),
            'service_icon': attachment.get('service_icon', ''),
            'id': attachment.get('id', 0),
            'original_url': attachment.get('original_url', ''),
            'fallback': attachment.get('fallback', ''),
            'text': attachment.get('text', ''),
            'title': attachment.get('title', ''),
            'title_link': attachment.get('title_link', ''),
            'service_name': attachment.get('service_name', ''),
            'fields': attachment.get('fields', []),
            'message_blocks': attachment.get('message_blocks', [])
        }

        if not all(extracted_attachment.values()):
            print(f"Empty or NoneType data found in attachment: {attachment}")

        extracted_attachments.append(extracted_attachment)

    return extracted_attachments

def extract_blocks(blocks):
    extracted_blocks = []

    for block in blocks:
        # Extract specific fields from the block
        extracted_block = {
            'type': block.get('type', ''),
            'block_id': block.get('block_id', ''),
            'elements': extract_elements(block.get('elements', []))
        }

        if not all(extracted_block.values()):
            print(f"Empty or NoneType data found in block: {block}")

        extracted_blocks.append(extracted_block)

    return extracted_blocks


def extract_messages_from_directory(directory):
    messages = []

    # Get the absolute path of the directory
    directory = os.path.abspath(directory)

    # Iterate through the directory and its subdirectories
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)

                # Read the JSON file
                with open(file_path, 'r') as f:
                    try:
                        json_data = json.load(f)

                        if json_data:
                            # Extract fields from the JSON data
                            extracted_data = extract_fields(json_data)
                            messages.append(extracted_data)
                        else:
                            print(f"Empty JSON data in file: {file_path}")

                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON in file {file_path}: {e}")

    return messages

def extract_fields(json_data):
    # Check if json_data is not None
    if json_data is None:
        print(f"NoneType data found: {json_data}")
        return {}

    # Extract specific fields from the JSON data
    extracted_data = {
        'client_msg_id': json_data.get('client_msg_id', ''),
        'type': json_data.get('type', ''),
        'text': json_data.get('text', ''),
        'user': json_data.get('user', ''),
        'ts': json_data.get('ts', ''),
        'team': json_data.get('team', ''),
        'user_team': json_data.get('user_team', ''),
        'source_team': json_data.get('source_team', ''),
        'user_profile': extract_user_profile(json_data.get('user_profile', {})),
        'attachments': extract_attachments(json_data.get('attachments', [])),
        'blocks': extract_blocks(json_data.get('blocks', [])),
        'thread_ts': json_data.get('thread_ts', ''),
        'reply_count': json_data.get('reply_count', 0),
        'reply_users_count': json_data.get('reply_users_count', 0),
        'latest_reply': json_data.get('latest_reply', ''),
        'reply_users': json_data.get('reply_users', []),
        'replies': json_data.get('replies', []),
        'is_locked': json_data.get('is_locked', False),
        'subscribed': json_data.get('subscribed', False),
        'reactions': json_data.get('reactions', [])
    }

    return extracted_data

def extract_user_profile(user_profile):
    # Check if user_profile is not None
    if user_profile is None:
        print(f"NoneType data found in user_profile: {user_profile}")
        return {}

    # Extract specific fields from the user profile
    extracted_profile = {
        'avatar_hash': user_profile.get('avatar_hash', ''),
        'image_72': user_profile.get('image_72', ''),
        'first_name': user_profile.get('first_name', ''),
        'real_name': user_profile.get('real_name', ''),
        'display_name': user_profile.get('display_name', ''),
        'team': user_profile.get('team', ''),
        'name': user_profile.get('name', ''),
        'is_restricted': user_profile.get('is_restricted', False),
        'is_ultra_restricted': user_profile.get('is_ultra_restricted', False)
    }

    return extracted_profile

def extract_attachments(attachments):
    extracted_attachments = []

    for attachment in attachments:
        # Ensure attachment is a dictionary
        if isinstance(attachment, dict):
            # Extract specific fields from the attachment
            extracted_attachment = {
                'from_url': attachment.get('from_url', ''),
                'image_url': attachment.get('image_url', ''),
                'image_width': attachment.get('image_width', 0),
                'image_height': attachment.get('image_height', 0),
                'image_bytes': attachment.get('image_bytes', 0),
                'service_icon': attachment.get('service_icon', ''),
                'id': attachment.get('id', 0),
                'original_url': attachment.get('original_url', ''),
                'fallback': attachment.get('fallback', ''),
                'text': attachment.get('text', ''),
                'title': attachment.get('title', ''),
                'title_link': attachment.get('title_link', ''),
                'service_name': attachment.get('service_name', ''),
                'fields': attachment.get('fields', []),
                'message_blocks': attachment.get('message_blocks', [])
            }

            extracted_attachments.append(extracted_attachment)
        else:
            print(f"Invalid attachment type found: {attachment}")

    return extracted_attachments

def extract_blocks(blocks):
    extracted_blocks = []

    for block in blocks:
        # Ensure block is a dictionary
        if isinstance(block, dict):
            # Extract specific fields from the block
            extracted_block = {
                'type': block.get('type', ''),
                'block_id': block.get('block_id', ''),
                'elements': extract_elements(block.get('elements', []))
            }

            extracted_blocks.append(extracted_block)
        else:
            print(f"Invalid block type found: {block}")

    return extracted_blocks

def extract_elements(elements):
    extracted_elements = []

    for element in elements:
        # Ensure element is a dictionary
        if isinstance(element, dict):
            # Extract specific fields from the element
            extracted_element = {
                'type': element.get('type', ''),
                'user_id': element.get('user_id', ''),
                'text': element.get('text', ''),
                'url': element.get('url', '')
            }

            extracted_elements.append(extracted_element)
        else:
            print(f"Invalid element type found: {element}")

    return extracted_elements




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
