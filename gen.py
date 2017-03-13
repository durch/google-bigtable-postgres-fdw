import json
import random
import string

def id_gen(N=64):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(N))

def coord_gen(rng=90):
    return random.randint(rng*-1, rng)

l = [{"id": id_gen(), "lat": coord_gen(90), "lng": coord_gen(180)} for _ in xrange(00)]
dt = json.dumps({"row_key": id_gen(), "column": id_gen(), "column_qualifier": id_gen(), "data": l})
with open("test.sql", "wb") as fp:
    fp.write("insert into test values('{}');".format(dt))
