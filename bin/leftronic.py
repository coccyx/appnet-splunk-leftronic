import sys, os
sys.path.append('lib')
sys.path.append(os.path.join('splunk-sdk-python'))

from config import Config
import logging, logging.handlers
import json
import urllib2, urllib
import time, datetime

import splunklib.client as client
import splunklib.results as results

from pprint import pprint

c = Config()
logger = logging.getLogger('appnet')

# Global for tracking posts this hour
pth_id = 0

def send_data(stream_name, point = None, command = None):
    data = {
        "accessKey": c.leftronic_access_key,
        "streamName": stream_name
    }
    
    if not point is None:
        data["point"] = point
    if not command is None:
        data["command"] = command

    request = urllib2.Request("https://beta.leftronic.com/customSend/",
        data = json.dumps(data)
    )
    logger.info("Sending data to Leftronic for %s.  Data: %s" % (stream_name, json.dumps(data)))
    response = urllib2.urlopen(request)


def top_talkers(service):
    query = "search sourcetype=appnet | top limit=10 user.username"
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1h", latest_time="rt")

    def iterate(job):
        logger.debug("Iterating top_talkers")
        reader = results.ResultsReader(job.preview())
        data = []

        for kind,result in reader:
            if kind == results.RESULT:
                data.append({
                    "name": result["user.username"],
                    "value": int(result["count"])
                })

        send_data(stream_name = "top_talkers", point = { "leaderboard": data })

    return (created_job, lambda job: iterate(job))

def top_posts(service):
    query = 'search sourcetype=appnet reply_to!=" null" | stats count by reply_to | sort 10 -count | rename reply_to AS id | join id [ search earliest=-4h sourcetype=appnet ] | table user.avatar_image.url, user.username, text, count'
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1h", latest_time="rt")

    def iterate(job):
        logger.debug("Iterating top_talkers")
        reader = results.ResultsReader(job.preview())
        data = []

        for kind,result in reader:
            if kind == results.RESULT:
                data.append({
                    "imgUrl": result["user.avatar_image.url"],
                    "title": result["user.username"],
                    "msg": result["text"]
                })

        send_data(stream_name = "top_posts", command = "clear")
        send_data(stream_name = "top_posts", point = data )

    return (created_job, lambda job: iterate(job))

def posts_today(service):
    query = "search sourcetype=appnet | stats count"
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1d@d", latest_time="rt")

    def iterate(job):
        logger.debug("Iterating posts_today")
        reader = results.ResultsReader(job.preview())

        for kind,result in reader:
            if kind == results.RESULT:
                point = result['count']

        send_data(stream_name = "posts_today", point = point)

    return (created_job, lambda job: iterate(job))

def posts_by_hour(service):
    def iterate(ignore):
        query = "search sourcetype=appnet | bucket span=1h _time | convert mktime(_time) as time | stats count by time"
        job = service.jobs.create(query, exec_mode="oneshot", earliest_time="-1d", latest_time="now")
        logger.debug("Iterating posts_by_hour")
        reader = results.ResultsReader(job)

        data = [ ]

        for kind,result in reader:
            if kind == results.RESULT:
                data.append({
                    "timestamp": long(result["time"]),
                    "number": long(result["count"])
                })

        send_data(stream_name = "posts_by_hour", command = "clear")
        send_data(stream_name = "posts_by_hour", point = data)

    return (None, lambda job: iterate(job))

def posts_this_hour(service):
    query = "search sourcetype=appnet | tail 1 | spath"
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-5m", latest_time="rt")

    def iterate(job):
        logger.debug("Iterating posts_this_hour.  Current pth_id: %s" % globals()['pth_id'])
        reader = results.ResultsReader(job.preview())

        data = [ ]
        count = 0

        for kind,result in reader:
            if kind == results.RESULT:
                if globals()['pth_id'] == 0:
                    globals()["pth_id"] = long(result["id"])
                count = long(result["id"]) - globals()['pth_id']
                globals()['pth_id'] = long(result["id"])

        send_data(stream_name = "posts_this_hour", point = count)

    return (created_job, lambda job: iterate(job))

def posts_per_minute(service):
    def iterate(ignore):
        query = "search sourcetype=appnet | stats count"
        job = service.jobs.create(query, exec_mode="oneshot", earliest_time="-1m", latest_time="now")
        logger.debug("Iterating posts_per_minute")
        reader = results.ResultsReader(job)

        data = [ ]
        count = 0

        for kind,result in reader:
            if kind == results.RESULT:
                count = long(result["count"])

        send_data(stream_name = "posts_per_minute", point = count)

    return (None, lambda job: iterate(job))

def top_mentions(service):
    query = "search sourcetype=appnet | top limit=10 entities.mentions{}.name"
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1h", latest_time="rt")

    def iterate(job):
        logger.debug("Iterating top_mentions")
        reader = results.ResultsReader(job.preview())

        data = [ ]

        for kind,result in reader:
            if kind == results.RESULT:
                data.append({
                    "name": result['entities.mentions{}.name'],
                    "value": result['count']
                })

        send_data(stream_name = "top_mentions", point = { "leaderboard": data })

    return (created_job, lambda job: iterate(job))

def top_hashtags(service):
    query = "search source=/opt/logs/appnet.log | spath output=hashtag path=entities.hashtags{}.name | eval hashtag=lower(hashtag) | top hashtag"
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1h", latest_time="rt")

    def iterate(job):
        logger.debug("Iterating top_hashtags")
        reader = results.ResultsReader(job.preview())

        data = [ ]

        for kind,result in reader:
            if kind == results.RESULT:
                data.append({
                    "name": result['hashtag'],
                    "value": result['count']
                })

        send_data(stream_name = "top_hashtags", point = {"leaderboard": data})

    return (created_job, lambda job: iterate(job))

def test(service):
    query = "search sourcetype=appnet | stats count"
    # job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1d", latest_time="rt")
    # 
    # reader = results.ResultsReader(job.preview())

    job = service.jobs.create(query, exec_mode="oneshot", earliest_time="-1m", latest_time="now")
    reader = results.ResultsReader(job)

    data = []

    for kind,result in reader:
        if kind == results.RESULT:
            pprint(result)

def main(argv):
    splunkargs = { "host": c.splunk_host,
                   "port": c.splunk_port,
                   "scheme": c.splunk_scheme,
                   "username": c.splunk_username,
                   "password": c.splunk_password }
    service = client.connect(**splunkargs)
    
    # This is the list of dashboard streams
    rt_streams = [
        top_talkers,
        posts_today,
        top_posts,
        posts_this_hour,
        posts_per_minute,
        top_mentions,
        top_hashtags
    ]
    hourly_streams = [
        posts_by_hour
    ]


    # test(service)
    # rt_streams = [ ]
    # hourly_streams = [ ]
    # rt_streams = [ posts_per_minute, posts_this_hour ]


    jobs = []
    iterators = []

    hourly_jobs = []
    hourly_iterators = []

    # For each stream, we get back the created job
    # that feeds the stream, and also the iterator
    # that will poll the job and forward the data
    # to the dashboard
    for stream in rt_streams:
        job, iterator = stream(service)
        jobs.append(job)
        iterators.append(iterator)

    for stream in hourly_streams:
        job, iterator = stream(service)
        hourly_jobs.append(job)
        hourly_iterators.append(iterator)

    try:
        while True:
            # For each (job,iterator) pair, we invoke the
            # iterator, which in turn will pull new results
            # from that job, and send them up to the dashboard

            # Execute these jobs once per hour, then execute the others every c.sleep interval
            # Until the hour changes

            for job, iterator in zip(hourly_jobs, hourly_iterators):
                iterator(job)

            hour = datetime.datetime.now().hour
            while datetime.datetime.now().hour == hour:
                for job, iterator in zip(jobs, iterators):
                    iterator(job)

                time.sleep(c.sleep)
                
    except KeyboardInterrupt:
        pass
    finally:
        for job in jobs:
            try:
                job.cancel()
            except:
                pass

if __name__ == "__main__":
    main(sys.argv)


