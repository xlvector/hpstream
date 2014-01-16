import urllib, datetime
import sys
import urllib2

def getHour(tm):
    return int(tm.split(':')[0])

def getTimeStamp(buf):
    dt = int(buf)
    year = dt / 10000
    month = (dt % 10000) / 100
    day = dt % 100
    return (datetime.datetime(year, month, day) - datetime.datetime(1970,1,1)).total_seconds()

def getDate(buf):
    dt = int(buf)
    year = dt / 10000
    month = (dt % 10000) / 100
    day = dt % 100
    return datetime.datetime(year, month, day)

def normName(region):
    return region.lower().replace(' ', '_').replace('+', '_').replace('-', '_').replace(':', '')

def contentId2VideoMap():
    lines = urllib2.urlopen('http://10.16.3.204/api_production/videos.tsv').read().split('\n')
    ret = {}
    for line in lines:
        if len(line.strip()) == 0:
            continue
        video_id, show_id, content_id, video_type = line.strip().split('\t')
        ret[content_id] = (show_id, video_type)
    return ret

def showAgeGroupMap():
    lines = urllib2.urlopen('http://10.16.3.204/api_production/shows.tsv').read().split('\n')
    ret = {}
    for line in lines:
        if len(line.strip()) == 0:
            continue
        show_id, age_group = line.strip().split('\t')
        ret[show_id] = age_group
    return ret

def showGenresMap():
    lines = urllib2.urlopen('http://10.16.3.204/api_production/show_genres.tsv').read().split('\n')
    ret = {}
    for line in lines:
        if len(line.strip()) == 0:
            continue
        tks = line.strip().split('\t')
        if len(tks) != 2:
            continue
        show_id, genre = tks
        kv = genre.split('~')
        if show_id not in ret:
            ret[show_id] = set()
        ret[show_id].add(kv[0])
    return ret


def dumps(vec):
    ret = ''
    for key, value in vec.items():
        ret += str(key) + ':' + str(value)
        ret += '\t'
    return ret

def loads(buf):
    tks = buf.strip().split('\t')
    ret = {}
    for tk in tks:
        kv = tk.split(':')
        ret[kv[0]] = kv[1]
    return ret

def parseUrl(url):
    if url == None or url.find('?') < 0:
        return [None, None]
    req, params = urllib.unquote(url).split('?', 1)
    ret = dict()
    tks = params.split('&')
    for tk in tks:
        if tk.find('=') < 0:
            continue
        key, value = tk.split('=', 1)
        ret[key] = value

    return [req, ret]

def shrinkUrl(url,keys):
    req, params = parseUrl(url)
    return req + '?' + '&'.join([x + '=' + y for x, y in params.items() if x in keys])

def getEvent(req):
    event = 'none'
    if req == '/v3/sitetracking/pageload':
        event = 'pageload'
    elif req == '/v3/sitetracking/pageinfo/load':
        event = 'trayload'
    elif req == '/v3/sitetracking/thumbnailclick':
        event = 'trayclick'
    elif req == '/v3/playback/start':
        event = 'playbackstart'
    elif req == '/v3/playback/end':
        event = 'playbackend'
    elif req == '/v3/playback/position':
        event = 'watched'
    elif req == '/v3/sitetracking/masthead/click':
        event = 'trayclick'

    return event

def getParameter(params, key):
    if key in params:
        return params[key].lower()
    return 'none'

def getIntParameter(params, key):
    value = getParameter(params, key)
    try:
        return int(value)
    except ValueError:
        return -1

def getFloatParameter(params, key):
    value = getParameter(params, key)
    try:
        return float(value)
    except ValueError:
        return 0


def matchRules(rules, params):
    for key, value in rules.items():
        if getParameter(params, key).lower() != value.lower():
            return False
    return True

def join(ch, elements):
    return ch.join([str(x) for x in elements])

def watchTime(watched):
    ret = watched
    if watched > 3600000:
        ret = 3600000
    if watched < 0:
        ret = 0
    return ret
