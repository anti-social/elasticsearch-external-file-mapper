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

Default Elasticsearch version is `6.2.3`. To build against other version run with option:

```
vagga assemble -PesVersion=6.1.4
```

To build for older Elasticsearch choose appropriate branch:

- `es-6.0` for Elasticsearch `6.0.x`
- `es-5.5` for Elasticsearch `5.5.x` or `5.6.x`

Run all the tests (unit, integration and functional):

```
vagga test
```

Also you can use gradle for everything, except functional tests.
