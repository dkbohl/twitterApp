import numpy as np
import networkx as nx
import pandas as pd
import json



def filter_for_largest_components(G, num_comp):
    G = nx.compose_all(sorted(nx.connected_component_subgraphs(G), key = len, reverse=True)[:num_comp])
    return G

def filter_for_k_core(G, k_cores):
    G.remove_edges_from(G.selfloop_edges())
    G_tmp = nx.k_core(G, k_cores)
    if len(G_tmp) <= 3:
        G_tmp = nx.k_core(G)
    return G_tmp

def getText1(x, df):
    length1 = len(df.loc[df["source"] == x]["text"])
    length2 = len(df.loc[df["target"] == x]["text"])
    if length1 > 0:
        text = df.loc[df["source"] == x]["text"].values[0]
        return text;
    elif length2 > 0:
        text = df.loc[df["target"] == x]["text"].values[0]
        return text;
    else:
        return;

def getTweetID(x, df):
    length1 = len(df.loc[df["source"] == x]["id"])
    length2 = len(df.loc[df["target"] == x]["id"])
    if length1 > 0:
        text = df.loc[df["source"] == x]["id"].values[0]
        return text;
    elif length2 > 0:
        text = df.loc[df["target"] == x]["id"].values[0]
        return text;
    else:
        return;


# This loads the most comprehensive text portion of the tweet  
def get_text(data):       
    # Try for extended text of original tweet, if RT'd (streamer)
    try: text = data['retweeted_status']['extended_tweet']['full_text']
    except: 
        # Try for extended text of an original tweet, if RT'd (REST API)
        try: text = data['retweeted_status']['full_text']
        except:
            # Try for extended text of an original tweet (streamer)
            try: text = data['extended_tweet']['full_text']
            except:
                # Try for extended text of an original tweet (REST API)
                try: text = data['full_text']
                except:
                    # Try for basic text of original tweet if RT'd 
                    try: text = data['retweeted_status']['text']
                    except:
                        # Try for basic text of an original tweet
                        try: text = data['text']
                        except: 
                            # Nothing left to check for
                            text = ''
    return text
   
def tweet_data_to_dataframe(data):
    tweets_out = []
    for t in data:
        tweets_out.extend(parse_tweet(t))
    tweets_out = pd.DataFrame(tweets_out)
    tweets_out.columns = ['from_user', 'from_user_name', 'mentioned_user', 'mentioned_user_name', 'tweet_type', 'tweet_text', 'tweet_url', 'timestamp']
    return tweets_out

def parse_tweet(data):
    tweet_metadata = []
    tweet_text = get_text(data)
    tweet_id = str(data['id_str'])
    tweet_url = 'https://twitter.com/anyuser/status/'+ tweet_id
    from_user = data['user']['screen_name']
    from_user_name = data['user']['name']
    from_user_description = data['user']['description']
    timestamp = data['timestamp_ms']
    #mentions = data['entities']['user_mentions']  #EXTEND TO EXTENDED MENTIONS
    if data['in_reply_to_status_id'] != None:
        tweet_type = 'reply'
        #status_id = data['in_reply_to_status_id']
        #status_id_str = data['in_reply_to_status_id_str']
        mentioned_user = data['in_reply_to_user_id']
        mentioned_user_id = data['in_reply_to_user_id_str']
        mentioned_user_name = data['in_reply_to_screen_name']  # This seems to be the handle rather than the moniker
        mentions = []
        tweet_metadata.append([from_user, from_user_name, mentioned_user_name, mentioned_user, tweet_type, tweet_text, tweet_url, timestamp])
    elif 'retweeted_status' in data:
        mentions = [data['entities']['user_mentions'][0]] # ADD ONLY THE FIRST (RT) MENTION
        tweet_type = 'retweet'
        tweet_id = data['retweeted_status']['id']
    elif len(data['entities']['user_mentions'])==0:
        tweet_type = 'root'
        mentions = []
        tweet_metadata.append([from_user, from_user_name, None, None, tweet_type, tweet_text, tweet_url, timestamp])
    else:
        mentions = data['entities']['user_mentions'] # ADD ALL, MUST EXTEND TO EXTENDED
        tweet_type = 'mention'
    for mention in mentions:
        tweet_type = tweet_type
        mentioned_user = mention['screen_name']
        mentioned_user_id = mention['id']
        mentioned_user_name = mention['name']
        tweet_metadata.append([from_user, from_user_name, mentioned_user, mentioned_user_name, tweet_type, tweet_text, tweet_url, timestamp])
    return tweet_metadata

def filter_for_largest_components(G, num_comp):
    if nx.is_directed(G)==True:
        G = nx.compose_all(sorted(nx.weakly_connected_component_subgraphs(G), key = len, reverse=True)[:num_comp])
    else:
        G = nx.compose_all(sorted(nx.connected_component_subgraphs(G), key = len, reverse=True)[:num_comp])
    return G

def filter_for_k_core(G, k_cores):
    G.remove_edges_from(G.selfloop_edges())
    G_tmp = nx.k_core(G, k_cores)
    if len(G_tmp) <= 3:
        G_tmp = nx.k_core(G)
    return G_tmp

def dataframe_to_graph(df):
    df = df[['from_user', 'mentioned_user']]
    edge_list = [tuple(x) for x in df.to_numpy()]
    G = nx.DiGraph()
    G.add_edges_from(edge_list)
    return G

def dataframe_to_graph_extended(df):
    df = df[['from_user', 'mentioned_user']]
    edge_list = [tuple(x) for x in df.to_numpy()]
    G = nx.DiGraph()
    G.add_edges_from(edge_list)
    return G

def get_tweet_layer_as_df(df, tweet_type):
    df_layer = df[df['tweet_type']==tweet_type]
    return df_layer

def get_tweet_cascade_by_layer(df_root, df_layer):
    df_cascade = df_layer.loc[df_layer['mentioned_user'].isin(df_root['from_user'])]
    return df_cascade

def get_longest_cascade_root(G):
    successor_dict = nx.dfs_successors(G)
    length_dict = {key: len(value) for key, value in successor_dict.items()}
    values = list(length_dict.values())
    keys = list(length_dict.keys())
    m = max(values)
    i = values.index(m)
    return keys[i]