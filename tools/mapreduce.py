import sys

def do_map(map_func, map_cleanup = None, map_setup = None, fin = sys.stdin, total = 1, index = 0, config = None):
    if config == None:
        config = dict()
    if map_setup != None:
        map_setup(config)

    k = 0
    for line in fin:
        k += 1
        if k % total != index:
            continue
        tks = line.strip().split('\t')
        key = tks[0]
        value = '\t'.join(tks[1:])
        map_func(key, value, config)
    if map_cleanup != None:
        map_cleanup(config)

def do_reduce(reduce_func, reduce_cleanup = None, reduce_setup = None, fin = sys.stdin, config = None):
    if config == None:
        config = dict()
    if reduce_setup != None:
        reduce_setup(config)
    prev_key = ''
    values = []
    for line in fin:
        tks = line.strip().split('\t')
        key = tks[0]
        value = '\t'.join(tks[1:])

        if prev_key != key:
            if len(prev_key) > 0 and len(values) > 0:
                reduce_func(prev_key, values, config)
            prev_key = key
            values = []
        values.append(value)
    if reduce_cleanup != None:
        reduce_cleanup(config)

