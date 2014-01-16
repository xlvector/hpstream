import sys
import mapreduce

def map_func(key, value, config):
    words = value.split(' ')
    for word in words:
        print word + '\t1'

def reduce_func(key, values, config):
    print key + '\t' + str(sum([int(x) for x in values]))

if sys.argv[1] == 'map':
    mapreduce.do_map(map_func = map_func)
elif sys.argv[1] == 'reduce':
    mapreduce.do_reduce(reduce_func = reduce_func)
