import collections
from heapq import heappush, heappop, nlargest

from datetime import datetime, timedelta
from dateutil import tz

import re
import math



class BlockedHostFilter:

    allowed_login_attempts = 3

    def __init__(self, past_interval, future_interval):

        self.past_dq = collections.deque(maxlen = past_interval)
        self.future_dq = collections.deque(maxlen = future_interval)
        self.current_blocked = None
        self.current_time = None

    '''
       Filter to be called before forwarding the response the request
    '''
    def filter_request(self, host, ts, rqst, resp):

        #executed once at initialization
        if self.current_blocked==None:
            self.current_blocked = self.current_invalid_logins = set()
            self.current_time = ts
            [self.future_dq.append(set()) for i in range(self.future_dq.maxlen)]

        time_diff =  math.ceil((ts - self.current_time).total_seconds())
        if time_diff > 0:
            # we need to move forward in time
            for t in range(time_diff):
                self.past_dq.append(self.current_invalid_logins)
                self.current_invalid_logins = set()
                self.current_blocked = self.future_dq.popleft()
                self.future_dq.append(set())
                self.current_time = self.current_time + timedelta(seconds=1)

        #Check if the host is blocked
        if host in self.current_blocked:
            #Log failed request here
            return False

        if rqst == '/login':
            if resp!='200':
                self.current_invalid_logins.add(host)

                if sum([host in failed_logins for failed_logins in self.past_dq]) == (self.allowed_login_attempts-1):
                    #blocks all future requests for desired time interval including login requests
                    [blocked_host_set.add(host) for blocked_host_set in self.future_dq]
                # For this program we return nothing, but we must forward an appropriate response on production

            else:
                self.current_blocked.discard(host)
                [failed_logins.discard(host) for failed_logins in self.past_dq]
                [blocked_hosts.discard(host) for blocked_hosts in self.future_dq]
                # For this program we return nothing, but we must forward an appropriate response on production

        return True



class WindowedQueue:
    def __init__(self, size, topNsize):

        # Queue of size (size) to maintain [timestamp, #request]
        self.dq = collections.deque(maxlen=size)

        '''
        Heap of size topN
        Heap to store [start_time, end_time, #requests] tuples
        Heaps evaluate in python based upon first element in tuple. Else write a custom class and overide __lt__ func. Saviour!
        https://docs.python.org/2/library/heapq.html#basic-examples
        '''
        self.h = []
        self.h_maxlen = topNsize

        #total_requests in the current queue
        self.current_total_requests = 0



    def add( self, new_timestamp, new_count ):
        self.dq.append([new_timestamp,new_count])
        self.current_total_requests += new_count

        #only if queue is completely filled. Handles initial entries.
        if len(self.dq) == self.dq.maxlen:
            #get necessary elements
            dq_left_node = self.dq.popleft()

            heappush(self.h,[self.current_total_requests, dq_left_node[0], new_timestamp])
            if(len(self.h)>self.h_maxlen):
                heappop(self.h)

            #left node is removed to maintain the size and thus updation of counter necessary
            self.current_total_requests -= dq_left_node[1]






filename = '../log_input/log.txt'
hostsfile = '../log_output/hosts.txt'
bandwidthfile = '../log_output/resources.txt'
busiestintervalsfile = '../log_output/hours.txt'
blocked_filename = '../log_output/blocked.txt'


splitting_char_extract_host = '- - ['
splitting_char_extract_resource = '\"'

valid_requests = ['GET','POST','HEAD','PUT','DELETE','CONNECT','OPTIONS','TRACE','PATCH']
#we use first 3 chars as valid-request identifiers.
valid_requests = set(token[:3] for token in valid_requests)

token_delimiter = " "

base_tz = tz.gettz('America/New_York')
timestamp_formatter = '%d/%b/%Y:%H:%M:%S %z'
timestamp_extractor = '[\[\]]'

def process(line):
    request_desc, response_desc = line.split(splitting_char_extract_resource)[1:3]
    request_type = request_desc[:3].upper()
    #We process and entertain only valid requests
    if request_type not in valid_requests:
            return

    host = line.split(splitting_char_extract_host)[0].strip()

    #Assuming all resourcepaths are absolute
    resourcename = request_desc.split(token_delimiter)[1].strip()
    resource_size = line.split(token_delimiter)[-1].strip()
    if resource_size=='-':
        resource_size = 0

    #Bring everything to Eastern Time
    dt = re.split('[\[\]]', line)[1]
    dt = datetime.strptime(dt,timestamp_formatter)
    dt = dt.astimezone(base_tz)

    resp = line.split(token_delimiter)[-2].strip()

    return [host, dt, resourcename, resource_size, resp]

with open(filename,encoding='latin-1') as f:
    host_counter = collections.Counter()

    resource_counter = collections.Counter()
    resource_data_counter = collections.Counter()

    bandwidth_counter  = collections.Counter()

    timestamp_counter = collections.Counter()
    #Used later to create filler range for timestamps that do not
    max_timestamp = 0
    min_timestamp = 0

    blocked_host_filter = BlockedHostFilter(20,5*60)

    with open(blocked_filename,'w') as bf:
        for line in f:
            try:
                host, rqst_time, resourcename, resource_size, resp = process(line)

                host_counter[host] += 1
                resource_counter[resourcename] += 1
                resource_data_counter[resourcename] += int(resource_size)

                if min_timestamp==0 or rqst_time < min_timestamp :
                    min_timestamp = rqst_time
                if max_timestamp==0 or rqst_time > max_timestamp:
                    max_timestamp = rqst_time
                timestamp_counter[rqst_time] += 1

                if not blocked_host_filter.filter_request(host, rqst_time, resourcename, resp):
                    #print(host, ts, rqst, resp)
                    bf.write(line)

            except:
                #discard the line
                pass

    #Feature 1
    #Processed in O(n) -> O(n) for Time to read + O(k log k) to get top-k-elements
    with open (hostsfile, 'w') as f:
        for k,v in host_counter.most_common(10):
            f.write(k+','+str(v)+'\n')

    #Feature 2
    # A trie-based structure would be more suitable if the site gets very-very large, which is almost impossible.

    #Processed in O(n) -> O(n) for Time to read + O(k log k) to get top-k-elements
    for k in resource_counter.keys():
        bandwidth_counter[k] = resource_data_counter[k]/resource_counter[k]

    with open (bandwidthfile, 'w') as f:
        for k,v in bandwidth_counter.most_common(10):
            f.write(k+'\n')


    #Feature 3
    # Time periods can overlap. If they are not to overlap, we need to do some more work
    #Processed in O(n) -> O(n) for Time to read + O(k log k) to get top-k-elements

    size = 60*60
    top = 10
    windowed_queue = WindowedQueue(size,top)


    numseconds = math.ceil((max_timestamp-min_timestamp).total_seconds())
    #Create a timeseries for min-max at with interval of 1 second to pad seconds_list
    seconds_list = []
    min_timestamp = min_timestamp - timedelta(seconds=1)
    temp = min_timestamp
    for x in range(0, numseconds):
        temp = temp + timedelta(seconds=1)
        seconds_list.append(temp)
    #padded seconds will return 0 from the counter
    for sec in seconds_list:

        windowed_queue.add(sec,timestamp_counter[sec])


    with open (busiestintervalsfile, 'w') as f:
        for rqst_cnt, window_start, window_end in nlargest(len(windowed_queue.h),windowed_queue.h):
            f.write(str(window_start)+','+str(rqst_cnt)+'\n')

