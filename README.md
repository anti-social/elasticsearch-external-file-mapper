[![Build Status](https://travis-ci.org/anti-social/elasticsearch-external-file-mapper.svg?branch=master)](https://travis-ci.org/anti-social/elasticsearch-external-file-mapper)

# External file field mapper for Elasticsearch

## How to build

Run gradle daemon:

```
vagga daemon
```

Assemble the project:

```
vagga assemble
```

Default Elasticsearch version is `5.6.8`. To build against other version run with option:

```
vagga assemble -PesVersion=5.5.3
```

Run all the tests (unit, integration and functional):

```
vagga test
```

Also you can use gradle for everything, except functional tests.
