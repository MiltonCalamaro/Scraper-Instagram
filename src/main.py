# -*- coding: utf-8 -*-
"""
Autor : MiltonCalamaro
"""
import datetime as dt
from itertools import dropwhile, takewhile
import instaloader
import pandas as pd
import re
import json
import os
import logging
import argparse
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger('__instagram__')
global USERNAME, PASSWORD
USERNAME = 'milton_calamaro'
PASSWORD = 'Apa123Apa123.'
def get_posts_hashtag(since, until, hashtag):
    L = instaloader.Instaloader( # Main Class info: https://instaloader.github.io/as-module.html#instaloader-main-class
        download_pictures=False,
        download_videos=False, 
        download_video_thumbnails=False,
        compress_json=False, 
        download_geotags=False, 
        post_metadata_txt_pattern="", 
        max_connection_attempts=0,
        download_comments=True,
        save_metadata = False,
        filename_pattern = '{mediaid}'
    )
    L.login(USERNAME, PASSWORD)
    # posts = L.get_hashtag_posts(hashtag) # instaloader.Hashtag.get_posts().
    posts = instaloader.Hashtag.from_name(L.context, hashtag).get_posts()
    print("capturing posts from: "+str(since)+" to: "+str(until))
    SINCE = dt.datetime.strptime(until, "%Y-%m-%d %H:%M:%S") + dt.timedelta(hours=+5)
    UNTIL = dt.datetime.strptime(since, "%Y-%m-%d %H:%M:%S") + dt.timedelta(hours=+5)
    list_post = []
    for post in takewhile(lambda p: p.date >= UNTIL, dropwhile(lambda p: p.date >= SINCE, posts)):
        dict_post = {}
        dict_post['post_id'] = post.mediaid
        dict_post['date'] = post.date_local
        dict_post['user_id'] = post.owner_id
        dict_post['user_name'] = post.owner_username
        dict_post['text'] = post.caption
        dict_post['post_url'] = f'https://www.instagram.com/p/{post.shortcode}/'
        dict_post['post_type'] = post.typename
        dict_post['hashtags'] = post.caption_hashtags
        dict_post['mentions'] = post.tagged_users
        dict_post['replies'] = post.comments
        dict_post['likes'] = post.likes
        dict_post['url_picture'] = post.url
        dict_post['location_id'] =  post.__dict__['_full_metadata_dict']['location']['id'] if post.__dict__['_full_metadata_dict']['location'] else ''
        dict_post['location_name'] = post.__dict__['_full_metadata_dict']['location']['name'] if post.__dict__['_full_metadata_dict']['location'] else ''
#         dict_post['location'] = post.location
        dict_post['is_video'] = post.is_video
        dict_post['video_duration'] = post.video_duration
        dict_post['video_url'] = post.video_url
        dict_post['video_view'] = post.video_view_count
        list_post.append(dict_post)
        logger.info(' {} | {} | {}'.format(post.date_local, post.mediaid, post.caption.replace('\n', ' ')))
        L.download_post(post, target=f'#{hashtag}')
    return pd.DataFrame(list_post)

def get_posts_profile(since, until, username):
    L = instaloader.Instaloader( # Main Class info: https://instaloader.github.io/as-module.html#instaloader-main-class
        download_pictures=False,
        download_videos=False, 
        download_video_thumbnails=False,
        compress_json=False, 
        download_geotags=False, 
        post_metadata_txt_pattern="", 
        max_connection_attempts=0,
        download_comments=True,
        save_metadata = False,
        filename_pattern = '{mediaid}'
    )
    L.login(USERNAME, PASSWORD)
    posts = instaloader.Profile.from_username(L.context, username).get_posts()
    SINCE = dt.datetime.strptime(until, "%Y-%m-%d %H:%M:%S") + dt.timedelta(hours=+5)
    UNTIL = dt.datetime.strptime(since, "%Y-%m-%d %H:%M:%S") + dt.timedelta(hours=+5)
    list_post = []
    for post in takewhile(lambda p: p.date >= UNTIL, dropwhile(lambda p: p.date >= SINCE, posts)):
        dict_post = {}
        dict_post['post_id'] = post.mediaid
        dict_post['date'] = post.date_local
        dict_post['user_id'] = post.owner_id
        dict_post['user_name'] = post.owner_username
        dict_post['text'] = post.caption
        dict_post['post_url'] = f'https://www.instagram.com/p/{post.shortcode}/'
        dict_post['post_type'] = post.typename
        dict_post['hashtags'] = post.caption_hashtags
        dict_post['mentions'] = post.tagged_users
        dict_post['replies'] = post.comments
        dict_post['likes'] = post.likes
        dict_post['url_picture'] = post.url
        dict_post['location'] = post.__dict__['_node']['location']
        dict_post['is_video'] = post.is_video
        dict_post['video_duration'] = post.video_duration
        dict_post['video_url'] = post.video_url
        dict_post['video_view'] = post.video_view_count
        list_post.append(dict_post)
        logger.info(' {} | {} | {}'.format(post.date_local, post.mediaid, post.caption.replace('\n', ' ')))
        L.download_post(post, target=f'{username}')
    return pd.DataFrame(list_post)  

def get_fields_comment(comment, parent_id, root_id):
    try: 
        is_verified = comment['owner']['is_verified']
    except:
        is_verified = ''
    dict_comment = {'comment_id':comment['id'], 
                    'created_at':dt.datetime.fromtimestamp(comment['created_at']),
                    'text':comment['text'],
                    'user_id':comment['owner']['id'],
                    'is_verified':is_verified,
                    'picture_url':comment['owner']['profile_pic_url'],
                    'user_name':comment['owner']['username'],
                    'likes_count':comment['likes_count'],
                    'parent_id':parent_id,
                    'root_id':root_id
                   }
    logger.info(' {} | {} | {}'.format(dict_comment['created_at'], dict_comment['comment_id'], dict_comment['text'].replace('\n', ' ')))
    return dict_comment

def get_list_comments(filename):
    root_id = re.sub(r'\D+', '', filename)
    with open(filename) as fp:
        data = json.load(fp)
    list_comment = []
    for comment in data:
        list_comment.append(get_fields_comment(comment, root_id, root_id))
        if comment['answers']:
            for replies in comment['answers']:
                list_comment.append(get_fields_comment(replies, comment['id'], root_id))
    return list_comment

def get_user_info(username):
    L = instaloader.Instaloader( # Main Class info: https://instaloader.github.io/as-module.html#instaloader-main-class
        download_pictures=False,
        download_videos=False, 
        download_video_thumbnails=False,
        compress_json=False, 
        download_geotags=False, 
        post_metadata_txt_pattern=None, 
        max_connection_attempts=2,
        download_comments=False,
    )
    # L.login(USERNAME, PASSWORD)
    dict_user_info = {}
    try: 
        profile = instaloader.Profile.from_username(L.context, username)
    except:
        L.login(USERNAME, PASSWORD)  
        profile = instaloader.Profile.from_username(L.context, username)     
    dict_user_info['user_id'] = profile.userid
    dict_user_info['user_name'] = profile.username
    dict_user_info['user_fullname'] = profile.full_name
    dict_user_info['profile_url'] = f'https://www.instagram.com/{profile.username}' 
    dict_user_info['picture_url'] = profile.profile_pic_url 
    dict_user_info['biography'] = profile.biography
    dict_user_info['followees_count'] = profile.followees
    dict_user_info['followers_count'] = profile.followers 
    dict_user_info['highlight_count'] = profile.__dict__['_node']['highlight_reel_count']
    dict_user_info['post_count'] = profile.mediacount 
    dict_user_info['igtv_count'] = profile.igtvcount 
    dict_user_info['is_verified'] = profile.is_verified 
    dict_user_info['is_private'] = profile.is_private
    dict_user_info['is_business_account'] = profile.is_business_account
    dict_user_info['business_category'] = profile.business_category_name
    dict_user_info['business_email'] = profile.__dict__['_node']['business_email']
    dict_user_info['category_enum'] = profile.__dict__['_node']['category_enum']
    dict_user_info['external_url'] = profile.external_url
    logger.info(f' {profile.userid} | {profile.username} | {profile.full_name}')
    return dict_user_info

def main(hashtag, profile, since, until, download_user):
    ### extraer post del hashtags
    # df_posts_hashtag = pd.DataFrame()
    if hashtag:
        logger.info(f'STARTING SCRAPPER POST WITH HASHTAG: {hashtag}')
        df_posts_hashtag = get_posts_hashtag(since, until, hashtag)
        df_posts_hashtag.to_csv(f'../results/df_posts_{hashtag}.csv', sep = '|', index = False, encoding='utf 8')
 
        logger.info("NOW, SCRAPPER COMMENTS OF THE POSTS")
        list_comment = []
        for filename in os.listdir(f'#{hashtag}'):
            list_comment.extend(get_list_comments(f'#{hashtag}/{filename}'))
        df_comments_hashtag = pd.DataFrame(list_comment)
        df_comments_hashtag.to_csv(f'../results/df_comments_{hashtag}.csv', sep = '|', index = False, encoding='utf 8')
    
    ### extraer post del perfil de usuario
    # df_posts_profile = pd.DataFrame()
    if profile:
        logger.info(f'STARTING SCRAPPER POST OF THE PROFILE: {profile}')
        df_posts_profile = get_posts_profile(since, until, profile)
        df_posts_profile.to_csv(f'../results/df_posts_{profile}.csv', sep = '|', index = False, encoding='utf 8')       
        
        logger.info("NOW, SCRAPPER COMMENTS OF THE POSTS")
        list_comment = []
        for filename in os.listdir(f'{profile}'):
            list_comment.extend(get_list_comments(f'{profile}/{filename}'))
        df_comments_profile = pd.DataFrame(list_comment)
        df_comments_profile.to_csv(f'../results/df_comments_{profile}.csv', sep = '|', index = False, encoding='utf 8')

    if download_user:
        ### extraer user_info de los tweets
        # usernames = pd.concat([df_posts_profile, df_posts_hashtag], axis=0, ignore_index = True) 
        df_posts_hashtag = pd.read_csv(f'../results/df_post_bancocentral.csv', sep = '|', encoding='utf 8')
        logger.info("LAST, EXTRACTION USER INFO")
        list_user_info = []
        for username in set(df_posts_hashtag['user_name']):
            list_user_info.append(get_user_info(username))
        df_user_info = pd.DataFrame(list_user_info)
        df_user_info.to_csv('../results/df_user_info.csv', sep = '|', index = False, encoding='utf 8')

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-hashtag',
                        dest = 'hashtag',
                        help = 'write a hashtags whithout #')
    parser.add_argument('-profile',
                        dest = 'profile',
                        help = 'write a hashtags whithout #')    
    parser.add_argument('-since',
                        dest = 'since',
                        help = 'define the start_date to scraper %Y-%m-%d %H:%M:%S',
                        type = str)
    parser.add_argument('-until',
                        dest = 'until',
                        help = 'define the last_date to scraper %Y-%m-%d %H:%M:%S',
                        type = str)
    parser.add_argument('--download_user',
                        dest = 'download_user',
                        help = 'set to download information of the user',
                        action = 'store_true')    
    args = parser.parse_args()
    main(args.hashtag, args.profile, args.since, args.until, args.download_user)
