import json
import random
import string

N_RECORDS = 100  # number of records generated
OUT_FILE = "test.sql"

# Be sure to modify the `column key below`

def id_gen(n=64):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))


def coord_gen(rng=90):
    return random.randint(rng * -1, rng)


if __name__ == "__main__":
    l = [{"row_key": id_gen(16), "family": 'cf1', "qualifier": 'test',
          "value": '{"id": %s, "lat": %d, ''"lng": %d)}' % (id_gen(32), coord_gen(90), coord_gen(180))}
         for _ in range(N_RECORDS)]

    with open(OUT_FILE, "wb") as fp:
        fp.write("insert into test values('{}');".format(json.dumps(l)))
