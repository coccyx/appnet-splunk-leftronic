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

import textwrap

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
        try:
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
        except KeyError, e:
            logger.debug("Received KeyError %s in top_talkers" % e)

    return (created_job, lambda job: iterate(job))

def top_posts(service):
    def iterate(job):
        try:
            logger.debug("Iterating top_posts")
            query = 'search sourcetype=appnet thread_id!=" null" | stats count by thread_id  | sort 10 -count | rename thread_id AS id | join id [ search earliest=-4h sourcetype=appnet ] | table user.avatar_image.url, user.username, id, text, count'
            job = service.jobs.create(query, exec_mode="blocking", earliest_time="-1h", latest_time="now")
            reader = results.ResultsReader(job.results())
            data = []

            html = """<style type=text/css">
                        A:link {text-decoration: none; color: #AAA}
                        A:visited {text-decoration: none; color: #AAA}
                        A:active {text-decoration: none; color: #AAA}
                        A:hover {text-decoration: underline; color: #AAA}
                   </style>"""
            temphtml = ""

            firsttime = True
            for kind,result in reader:
                if kind == results.RESULT:
                    temphtml += '<a target="_blank" href="http://alpha.app.net/%s/post/%s">' % (result["user.username"], result["id"])
                    if firsttime:
                        temphtml += """<div class="textFeedContainer" style="margin-top: 46px; width: 398.4px; 
                                margin-left: 16.799999999999997px; margin-right: 16.799999999999997px; 
                                opacity: 1; height: 31.81666660308838px; padding-bottom: 8.399999999999999px; 
                                border-top-width: 1px;">"""
                        firsttime = False
                    else:
                        temphtml += """<div class="textFeedContainer" style="margin-top: 0px; width: 398.4px; 
                                margin-left: 16.799999999999997px; margin-right: 16.799999999999997px; 
                                opacity: 1; height: 47.81666660308838px; padding-bottom: 8.399999999999999px; 
                                border-top-width: 1px;">"""

                    temphtml += """<img src="%s" style="position: absolute; top: 4.199999999999999px; z-index: 350; 
                                width: 28.799999999999997px;" />
                                <div class="textFeedItem" style="position: relative; top: 0px; z-index: 350; 
                                font-size: 13.440000000000001px; line-height: 16.128px; width: 365.4px; 
                                left: 33px; padding-top: 4.199999999999999px;">
                                <span class="textFeedTitle" style="margin-right: 6.720000000000001px;">%s</span>
                                <span class="textFeedBody">%s</span>
                                </div>
                                </div>""" % (result["user.avatar_image.url"], result["user.username"], \
                                                result["text"])
                    temphtml += "</a>"
                    html += textwrap.dedent(temphtml)
                    temphtml = ""

            html += """<div class="widgetTitle" style="width: 398.4px; left: 16.799999999999997px;
                    'font-size: 16.799999999999997px; line-height: 48px;">Top Posts (Last 1H)</div>

                    <div class="widgetBackground" style="border-top-left-radius: 5px; border-top-right-radius: 5px;
                     border-bottom-left-radius: 5px; border-bottom-right-radius: 5px; top: 3px; left: 3px; 
                     width: 426px; height: 378px; opacity: 0;"></div>"""

            send_data(stream_name = "top_posts", command = "clear")
            send_data(stream_name = "top_posts", point = {"html": html} )
        except KeyError, e:
            logger.debug("Received KeyError %s in top_posts" % e)

    return (None, lambda job: iterate(job))

def posts_today(service):
    query = "search sourcetype=appnet | stats count"
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-0d@d", latest_time="rt")

    def iterate(job):
        try:
            logger.debug("Iterating posts_today")
            reader = results.ResultsReader(job.preview())

            for kind,result in reader:
                if kind == results.RESULT:
                    point = result['count']

            send_data(stream_name = "posts_today", point = point)
        except KeyError, e:
            logger.debug("Received KeyError %s in posts_today" % e)

    return (created_job, lambda job: iterate(job))

def unique_users(service):
    query = "search sourcetype=appnet | stats dc(user.username) as count"
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-0d@d", latest_time="rt")

    def iterate(job):
        try:
            logger.debug("Iterating unique_users")
            reader = results.ResultsReader(job.preview())

            for kind,result in reader:
                if kind == results.RESULT:
                    point = result['count']

            send_data(stream_name = "unique_users", point = point)
        except KeyError, e:
            logger.debug("Received KeyError %s in unique_users" % e)

    return (created_job, lambda job: iterate(job))

def posts_by_hour(service):
    def iterate(ignore):
        logger.debug("Iterating posts_by_hour")
        query = "search sourcetype=appnet | bucket span=15m _time | convert mktime(_time) as time | stats count by time"
        job = service.jobs.create(query, exec_mode="blocking", earliest_time="-1d", latest_time="now", max_results=1000000)
        reader = results.ResultsReader(job.results(count=500))

        data = [ ]

        try:
            for kind,result in reader:
                if kind == results.RESULT:
                    data.append({
                        "timestamp": long(result["time"]),
                        "number": long(result["count"])
                    })

            send_data(stream_name = "posts_by_hour", command = "clear")
            send_data(stream_name = "posts_by_hour", point = data)
        except KeyError, e:
            logger.debug("Received KeyError %s in posts_by_hour" % e)

    return (None, lambda job: iterate(job))

def posts_this_hour(service):
    query = "search sourcetype=appnet | tail 1 | spath"
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-5m", latest_time="rt")

    def iterate(job):
        logger.debug("Iterating posts_this_hour.  Current pth_id: %s" % globals()['pth_id'])
        reader = results.ResultsReader(job.preview())

        data = [ ]
        count = 0

        try:
            for kind,result in reader:
                if kind == results.RESULT:
                    if globals()['pth_id'] == 0:
                        globals()["pth_id"] = long(result["id"])
                    count = long(result["id"]) - globals()['pth_id']
                    globals()['pth_id'] = long(result["id"])

            send_data(stream_name = "posts_this_hour", point = count)
        except KeyError, e:
            logger.debug("Received KeyError %s in posts_this_hour" % e)

    return (created_job, lambda job: iterate(job))

def posts_per_minute(service):
    def iterate(ignore):
        try:
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
        except KeyError, e:
            logger.debug("Received KeyError %s in posts_per_minute" % e)

    return (None, lambda job: iterate(job))

def top_mentions(service):
    query = "search sourcetype=appnet | top limit=10 entities.mentions{}.name"
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1h", latest_time="rt")

    def iterate(job):
        try:
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
        except KeyError, e:
            logger.debug("Received KeyError %s in top_mentions" % e)

    return (created_job, lambda job: iterate(job))

def top_hashtags(service):
    query = "search source=/opt/logs/appnet.log | spath output=hashtag path=entities.hashtags{}.name | eval hashtag=lower(hashtag) | top hashtag"
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1h", latest_time="rt")

    def iterate(job):
        try:
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
        except KeyError, e:
            logger.debug("Received KeyError %s in top_hashtags" % e)

    return (created_job, lambda job: iterate(job))

def posts_by_clienttype(service):
    query = 'search sourcetype=appnet | lookup clients name AS source.name | fillnull value="Unknown" category | top category'
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1d", latest_time="rt")

    def iterate(job):
        logger.debug("Iterating posts_by_clienttype")
        reader = results.ResultsReader(job.preview())

        data = [ ]

        try:
            for kind,result in reader:
                if kind == results.RESULT:
                    data.append({
                        "name": result['category'],
                        "value": result['percent']
                    })

            send_data(stream_name = "posts_by_clienttype", point = {"chart": data})
        except KeyError, e:
            logger.debug("Received KeyError %s in posts_by_clienttype" % e)

    return (created_job, lambda job: iterate(job))

def avg_msg_length(service):
    query = "search sourcetype=appnet | eval txtlen=len(text) | stats avg(txtlen) as avgtxtlen"
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1d", latest_time="rt")

    def iterate(job):
        try:
            logger.debug("Iterating avg_msg_length")
            reader = results.ResultsReader(job.preview())

            data = [ ]

            for kind,result in reader:
                if kind == results.RESULT:
                    point = result['avgtxtlen']

            send_data(stream_name = "avg_msg_length", point = point)
        except KeyError, e:
            logger.debug("Received KeyError %s in avg_msg_length" % e)

    return (created_job, lambda job: iterate(job))


def posts_by_client(service):
    query = 'search sourcetype=appnet | top limit=10 source.name'
    created_job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1d", latest_time="rt")

    def iterate(job):
        try:
            logger.debug("Iterating posts_by_client")
            reader = results.ResultsReader(job.preview())

            data = [ ]

            for kind,result in reader:
                if kind == results.RESULT:
                    data.append({
                        "name": result['source.name'],
                        "value": result['percent']
                    })

            send_data(stream_name = "posts_by_client", point = {"chart": data})
        except KeyError, e:
            logger.debug("Received KeyError %s in posts_by_clienttype" % e)

    return (created_job, lambda job: iterate(job))

def posts_by_day(service):
    def iterate(ignore):
        try:
            logger.debug("Iterating posts_by_day")
            query = "search sourcetype=appnet | bucket span=1day _time | convert mktime(_time) as time | stats count by time"
            job = service.jobs.create(query, exec_mode="blocking", earliest_time="-7d@d", latest_time="now", max_results=1000000)
            reader = results.ResultsReader(job.results(count=500))

            data = [ ]

            for kind,result in reader:
                if kind == results.RESULT:
                    data.append({
                        "timestamp": long(result["time"]),
                        "number": long(result["count"])
                    })

            send_data(stream_name = "posts_by_day", command = "clear")
            send_data(stream_name = "posts_by_day", point = data)
        except KeyError, e:
            logger.debug("Received KeyError %s in posts_by_day" % e)

    return (None, lambda job: iterate(job))

def top_reposts(service):
    def iterate(job):
        try:
            logger.debug("Iterating top_reposts")
            query = """search sourcetype=appnet | rex mode=sed "s/\\\\r\\\\n/\\\\n/g" |
                    rex field=origtext mode=sed "s/\\\\\\\"([^\\\"]+)\\\\\\\"/\\\\1/" | 
                    eval asciitext=substr(text, 0) | rex field=asciitext 
                    ".*((u00bb|u267b|RP|Shared|>>) [:]*@[^: ]+[: ]*)(?P<origtext>.*)$" | 
                    eval origtext=trim(origtext) | stats last(asciitext), count, 
                    last(user.avatar_image.url) as user.avatar_image.url,
                    last(user.username) as user.username, last(id) as id by origtext | sort 10 - count |
                    table user.avatar_image.url, user.username, id, origtext, count"""
            print query
            query = textwrap.dedent(query)
            job = service.jobs.create(query, exec_mode="blocking", earliest_time="-24h", latest_time="now")
            reader = results.ResultsReader(job.results())
            data = []

            html = """<style type=text/css">
                        A:link {text-decoration: none; color: #AAA}
                        A:visited {text-decoration: none; color: #AAA}
                        A:active {text-decoration: none; color: #AAA}
                        A:hover {text-decoration: underline; color: #AAA}
                   </style>"""
            temphtml = ""

            firsttime = True
            for kind,result in reader:
                if kind == results.RESULT:
                    temphtml += '<a target="_blank" href="http://alpha.app.net/%s/post/%s">' % (result["user.username"], result["id"])
                    if firsttime:
                        temphtml += """<div class="textFeedContainer" style="margin-top: 46px; width: 398.4px; 
                                margin-left: 16.799999999999997px; margin-right: 16.799999999999997px; 
                                opacity: 1; height: 31.81666660308838px; padding-bottom: 8.399999999999999px; 
                                border-top-width: 1px;">"""
                        firsttime = False
                    else:
                        temphtml += """<div class="textFeedContainer" style="margin-top: 0px; width: 398.4px; 
                                margin-left: 16.799999999999997px; margin-right: 16.799999999999997px; 
                                opacity: 1; height: 47.81666660308838px; padding-bottom: 8.399999999999999px; 
                                border-top-width: 1px;">"""

                    temphtml += """<img src="%s" style="position: absolute; top: 4.199999999999999px; z-index: 350; 
                                width: 28.799999999999997px;" />
                                <div class="textFeedItem" style="position: relative; top: 0px; z-index: 350; 
                                font-size: 13.440000000000001px; line-height: 16.128px; width: 365.4px; 
                                left: 33px; padding-top: 4.199999999999999px;">
                                <span class="textFeedTitle" style="margin-right: 6.720000000000001px;">%s</span>
                                <span class="textFeedBody">%s</span>
                                </div>
                                </div>""" % (result["user.avatar_image.url"], result["user.username"], \
                                                result["origtext"])
                    temphtml += "</a>"
                    html += textwrap.dedent(temphtml)
                    temphtml = ""

            html += """<div class="widgetTitle" style="width: 398.4px; left: 16.799999999999997px;
                    'font-size: 16.799999999999997px; line-height: 48px;">Top Reposts (Last 24H)</div>

                    <div class="widgetBackground" style="border-top-left-radius: 5px; border-top-right-radius: 5px;
                     border-bottom-left-radius: 5px; border-bottom-right-radius: 5px; top: 3px; left: 3px; 
                     width: 426px; height: 378px; opacity: 0;"></div>"""

            send_data(stream_name = "top_reposts", command = "clear")
            send_data(stream_name = "top_reposts", point = {"html": html} )
        except KeyError, e:
            logger.debug("Received KeyError %s in top_reposts" % e)

    return (None, lambda job: iterate(job))

def test(service):
    query = "search sourcetype=appnet | bucket span=5m _time | convert mktime(_time) as time | stats count by time"
    # job = service.jobs.create(query, search_mode="realtime", earliest_time="rt-1d", latest_time="rt")
    # 
    # reader = results.ResultsReader(job.preview())

    job = service.jobs.create(query, exec_mode="blocking", earliest_time="-1d", latest_time="now")
    reader = results.ResultsReader(job.results(count=500))

    data = []

    for kind,result in reader:
        if kind == results.RESULT:
            pprint(result)

def main(argv):
    logger.debug("Splunk->Leftronic Started")
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
        #posts_per_minute,
        top_mentions,
        top_hashtags,
        unique_users,
        posts_by_clienttype,
        avg_msg_length
        #top_reposts
    ]
    hourly_streams = [
        posts_by_hour
        #posts_by_day
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

            minute = datetime.datetime.now().minute - (datetime.datetime.now().minute % 15)
            while (datetime.datetime.now().minute - (datetime.datetime.now().minute % 15)) == minute:
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


