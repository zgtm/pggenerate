# PgGenerate

Generate random data for your postgres database.

It will automatically find foreign key relationships and set the value to random foreign keys.

Currenty only a few types are supported, but support for other might be added on request.

## Usage

```
pggenerate <connection string> [optional parameters]
```

The connection string must be build according to the [libpq documentation](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING), e.g.

```
pggenerate "host=localhost dbname=myproject user=myusername password=mysecretpassword"
```

Possible parameters are:

 - `--only=table`: Only generate entries for table `table`. Can be repeated.
 - `--skip=table`: Don't generate entries for table `table`. Can be repeated.
 - `--require-after=table,column,aftertable,aftercolumn`: Whenever inserting a new entry into `table`, also insert a new entry into `aftertable` in the same transaction where `aftercolumn` will be set to the value of `column` of the new entry in `table`.
 - `--require-after=table,column,aftertable,aftercolumn`: Whenever inserting a new entry into `table`, first insert a new entry into `beforetable` in the same transaction. `column` will be set to the value of `beforecolumn` of the new entry in `beforetable`.


## Examples

### Simple example with foreign keys
First, create some tables:

```
CREATE TABLE cities (
    id   bigserial PRIMARY KEY,
    name text
);

CREATE TABLE weather (
    city_id  bigint NOT NULL REFERENCES cities,
    at       timestamptz NOT NULL,
    temp     int NOT NULL,
    comment  text
);
```

Then, run `pggenerate` for some seconds:

```
pggenerate "host=localhost dbname=myproject user=myusername password=mysecretpassword"
…
CTRL-C
```

And see the results:

```
=> SELECT count(*) FROM cities;
 count
-------
   229
(1 row)

=> SELECT count(*) FROM weather;
 count
-------
   219
(1 row)

=> SELECT * FROM cities LIMIT 10;
 id  |                       name
-----+--------------------------------------------------
 139 | duZBrEfbaGIzU2w
 140 | KCxBho6BdfulOOOqVY
 141 | 67q8aI0fwKDnz1jWEq0bbMCIzfRGyZ1UeIAijtnAzf1TfK5h
 142 | mfz3qYzic72Lz
   0 |
 143 | 󈩠򃄠𝛞
 144 |
 145 |
 146 |
 147 | aqUHHexg7T6bnp8Wj2SUY4ZSiT
(10 rows)

=> SELECT * FROM weather LIMIT 10;
 city_id |              at               |  temp  |                 comment
---------+-------------------------------+--------+-----------------------------------------
     140 | 2024-03-18 21:13:56.379866+00 |    993 | mAmCcIjuE3zarwfDgo6W9
         | 2024-03-18 21:13:56.407546+00 |      0 |
     143 | 2024-03-18 21:13:56.412652+00 | 271697 |
     139 | 2024-03-18 21:13:56.4214+00   | 126833 |
     140 | 2024-03-18 21:13:56.428279+00 |    223 |
       0 | 2024-03-18 21:13:56.435531+00 |      0 |
     142 | 2024-03-18 21:13:56.451897+00 |  91190 | 5NAemWNYpGdvdhoOgD0aL6E5XpV
     140 | 2024-03-18 21:13:56.463154+00 |      0 | WaSV7qdwVkcIg3org1fZlxbJ8g30sAJDIEk2yMi
     141 | 2024-03-18 21:13:56.471983+00 |  63519 | 5ZJiy0KCWA
     144 | 2024-03-18 21:13:56.480909+00 |      0 | 0VkmiRd
(10 rows)
```

