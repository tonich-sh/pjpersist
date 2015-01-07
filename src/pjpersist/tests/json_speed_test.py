"""
taken from: http://justinfx.com/2012/07/25/python-2-7-3-serializer-speed-comparisons/
"""

import pprint
from timeit import timeit

setup_orig = '''d = {
    'words': """
        Lorem ipsum dolor sit amet, consectetur adipiscing
        elit. Mauris adipiscing adipiscing placerat.
        Vestibulum augue augue,
        pellentesque quis sollicitudin id, adipiscing.
        """,
    'list': range(100),
    'dict': dict((str(i),'a') for i in xrange(100)),
    'int': 100,
    'float': 100.123456
}'''

# this is the data used in performance.py, BIGDICT ran through
# ObjectWriter(None).get_state(BIGDICT)
setup_profile = '''d = {
    'chant': 6785,
    'arraignments': True,
    'spoils': False,
    'inaugural': {
      'member': False,
      'domed': 3535,
      'Horacio': {
        '_py_type': 'datetime.datetime',
        'value': '1923-06-02T15:05:00'
      },
      'chimeras': {
        '_py_type': 'datetime.datetime',
        'value': '1967-03-01T13:42:00'
      },
      'Utopians': True
    },
    'lava': 'Domingo',
    'subteens': False,
    'Alcestis': 'mantilla',
    'bosh': 6909,
    'garbanzo': {
      'sachet': {
        'failure': {
          '_py_type': 'datetime.datetime',
          'value': '1975-07-22T04:44:00'
        },
        'breeding': 9051,
        'bankrolls': True,
        'imposed': 'avenged',
        'undeceived': {
          'poor': False,
          'festal': 3225,
          'reprinted': {
            '_py_type': 'datetime.datetime',
            'value': '1918-03-03T08:35:00'
          },
          'sleepwalkers': 'embarrassing',
          'intermezzi': 1484
        }
      },
      'benign': {
        'substantiates': {
          'underdeveloped': True,
          'buckskins': 'legalize',
          'Pepsi': True,
          'affords': 5854,
          'syphilis': False
        },
        'interaction': {
          '_py_type': 'datetime.datetime',
          'value': '1980-07-23T08:06:00'
        },
        'revision': True,
        'easygoing': {
          'replication': {
            '_py_type': 'datetime.datetime',
            'value': '1918-03-03T08:35:00'
          },
          'ointment': {
            'Talleyrand': False,
            'pinked': False,
            'medalist': {},
            'comparably': 'hedgehog',
            'goiter': 6895
          },
          'nonpolluting': 'Gretchen',
          'ripsaw': 4316,
          'basted': 'counterpoint'
        },
        'renaissances': {
          '_py_type': 'datetime.datetime',
          'value': '2013-01-22T18:59:00'
        }
      },
      'pennants': {
        'armadillo': {
          'civvies': 9109,
          'Condillac': 'Equuleus',
          'nurture': 1718,
          'Orly': {
            '_py_type': 'datetime.datetime',
            'value': '1911-01-17T04:24:00'
          },
          'hilly': {
            '_py_type': 'datetime.datetime',
            'value': '2013-01-22T18:59:00'
          }
        },
        'kleptomania': 'pounced',
        'comers': 322,
        'furtherance': {
          'Whitehead': False,
          'joysticks': {
            'drinker': 6444,
            'gamed': 'hosing',
            'larkspur': 3430,
            'prepaid': 'unspoiled',
            'stockpile': {
              '_py_type': 'datetime.datetime',
              'value': '1923-06-02T15:05:00'
            }
          },
          'Brahe': {
            '_py_type': 'datetime.datetime',
            'value': '1980-07-23T08:06:00'
          },
          'studding': {
            'illicitness': 7397,
            'rosin': False,
            'Botticelli': 8214,
            'Uruguayans': 830,
            'underskirts': False
          },
          'snarls': 'meditates'
        },
        'immodestly': 4678
      },
      'Armenian': True,
      'tryst': True
    },
    'potash': 7166
}'''

setup = setup_orig

setup_pickle    = '%s ; import cPickle ; src = cPickle.dumps(d)' % setup
setup_pickle2   = '%s ; import cPickle ; src = cPickle.dumps(d, 2)' % setup
setup_json      = '%s ; import json; src = json.dumps(d)' % setup
setup_msgpack   = '%s ; src = msgpack.dumps(d)' % setup

tests = [
    # (title, setup, enc_test, dec_test)
    ('pickle (ascii)', 'import pickle; %s' % setup_pickle, 'pickle.dumps(d, 0)', 'pickle.loads(src)'),
    ('pickle (binary)', 'import pickle; %s' % setup_pickle2, 'pickle.dumps(d, 2)', 'pickle.loads(src)'),
    ('cPickle (ascii)', 'import cPickle; %s' % setup_pickle, 'cPickle.dumps(d, 0)', 'cPickle.loads(src)'),
    ('cPickle (binary)', 'import cPickle; %s' % setup_pickle2, 'cPickle.dumps(d, 2)', 'cPickle.loads(src)'),
    ('json', 'import json; %s' % setup_json, 'json.dumps(d)', 'json.loads(src)'),
    ('simplejson', 'import simplejson; %s' % setup_json, 'simplejson.dumps(d)', 'simplejson.loads(src)'),
    #('python-cjson-1.0.5', 'import cjson; %s' % setup_json, 'cjson.encode(d)', 'cjson.decode(src)'),
    ('ujson', 'import ujson; %s' % setup_json, 'ujson.dumps(d)', 'ujson.loads(src)'),
    #('yajl 0.3.5', 'import yajl; %s' % setup_json, 'yajl.dumps(d)', 'yajl.loads(src)'),
    #('msgpack-python-0.3.0', 'import msgpack; %s' % setup_msgpack, 'msgpack.dumps(d)', 'msgpack.loads(src)'),
]

loops = 15000
enc_table = []
dec_table = []

print "Running tests (%d loops each)" % loops

for title, mod, enc, dec in tests:
    print title

    print "  [Encode]", enc
    result = timeit(enc, mod, number=loops)
    enc_table.append([title, result])

    print "  [Decode]", dec
    result = timeit(dec, mod, number=loops)
    dec_table.append([title, result])

enc_table.sort(key=lambda x: x[1])
enc_table.insert(0, ['Package', 'Seconds'])

for x in enc_table:
    x[0] = x[0].ljust(20)

dec_table.sort(key=lambda x: x[1])
dec_table.insert(0, ['Package', 'Seconds'])

for x in dec_table:
    x[0] = x[0].ljust(20)

print "\nEncoding Test (%d loops)" % loops
pprint.pprint(enc_table)

print "\nDecoding Test (%d loops)" % loops
pprint.pprint(dec_table)


results = """
results with setup_orig:
------------------------
Encoding Test (15000 loops)
[['Package             ', 'Seconds'],
 ['ujson               ', 0.15531587600708008],
 ['cPickle (binary)    ', 0.18461894989013672],
 ['json                ', 0.4842488765716553],
 ['simplejson          ', 0.6233742237091064],
 ['cPickle (ascii)     ', 0.9148178100585938],
 ['pickle (ascii)      ', 8.630627870559692],
 ['pickle (binary)     ', 8.879431009292603]]

Decoding Test (15000 loops)
[['Package             ', 'Seconds'],
 ['cPickle (binary)    ', 0.23794102668762207],
 ['ujson               ', 0.24951386451721191],
 ['simplejson          ', 0.41387104988098145],
 ['cPickle (ascii)     ', 0.7791750431060791],
 ['json                ', 0.9328320026397705],
 ['pickle (binary)     ', 3.062810182571411],
 ['pickle (ascii)      ', 9.76430606842041]]

results with setup_profile:
----------------------------
Encoding Test (15000 loops)
[['Package             ', 'Seconds'],
 ['ujson               ', 0.18154096603393555],
 ['cPickle (binary)    ', 0.33770203590393066],
 ['json                ', 0.38971710205078125],
 ['simplejson          ', 0.6901121139526367],
 ['cPickle (ascii)     ', 0.928969144821167],
 ['pickle (ascii)      ', 6.54378604888916],
 ['pickle (binary)     ', 7.975723028182983]]

Decoding Test (15000 loops)
[['Package             ', 'Seconds'],
 ['ujson               ', 0.23072004318237305],
 ['cPickle (binary)    ', 0.26100707054138184],
 ['simplejson          ', 0.2880890369415283],
 ['cPickle (ascii)     ', 0.4758889675140381],
 ['json                ', 0.891761064529419],
 ['pickle (binary)     ', 3.5709359645843506],
 ['pickle (ascii)      ', 6.168566942214966]]

"""
