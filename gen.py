import json
import random
import string

N_RECORDS = 1000 # number of records generated
OUT_FILE = "test.sql"

# Be sure to modify the `column key below`

def id_gen(N=64):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(N))

def coord_gen(rng=90):
    return random.randint(rng*-1, rng)

if __name__ = "__main__":

    l = [{"id": id_gen(), "lat": coord_gen(90), "lng": coord_gen(180)} for _ in xrange(N_RECORDS)]

    dt = json.dumps({"row_key": id_gen(), "column": 'cf1', "column_qualifier": 'test', "data": l})
    with open(OUT_FILE, "wb") as fp:
        fp.write("insert into test values('{}');".format(dt))
