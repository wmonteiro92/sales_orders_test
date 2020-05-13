# -*- coding: utf-8 -*-
"""
Created on Sat May  2 13:57:38 2020

@author: wmont
"""

import re

line_ending = ',,,,,,'
file = 'olist_order_reviews_dataset.csv'
file_engineered = 'olist_order_reviews_dataset_engineered.csv'

def modify_row(row_contents):
    pattern = r"(?P<pre>[a-z0-9]{32},[a-z0-9]{32},[0-9\.]*,)(?P<title>(\"[^\"]*?\")|([^\,]*?)),(?P<comment>.*?)(?P<post>,(?P<date1>20[0-9]{2}-[0-9]{2}-[0-9]{2}\s[0-9]{2}\:[0-9]{2}\:[0-9]{2})?,(?P<date2>20[0-9]{2}-[0-9]{2}-[0-9]{2}\s[0-9]{2}\:[0-9]{2}\:[0-9]{2})?,{6})"
            
    try:
        row_contents = row_contents.replace('\n', ' ')
        pre = re.match(pattern, row_contents)['pre']
        post = re.match(pattern, row_contents)['post']
        title = re.match(pattern, row_contents)['title']
        comment = re.match(pattern, row_contents)['comment']
        
        title = '"' + title.replace('"', "'") + '"' \
        if len(title) > 0 and not title.startswith('"') \
        else title
        
        comment = '"' + comment.replace('"', "'") + '"' \
        if len(comment) > 0 and not comment.startswith('"') \
        else comment
        
        return pre + title + ',' + comment + post.rstrip(',,,,,,') + '\n'
    except:
        return row_contents.rstrip().rstrip(',,,,,,') + '\n'
    
fe = open(file_engineered, mode='w+', encoding='utf-8')
with open(file, mode='r', encoding='utf-8') as f:
    row_contents = ''
    row_end_line = False
    first_line = True #flag required since in the first row the commas might not exist
    
    for line in f:
        row_contents += line
        
        if first_line or line.endswith(line_ending + '\n'):
            fe.writelines(modify_row(row_contents))
            row_contents = '' # clearing for the next row
            
        if first_line:
            first_line = False
        
fe.close()
