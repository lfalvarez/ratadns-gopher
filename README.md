# RaTA DNS - Gopher [![Build Status](https://travis-ci.org/niclabs/ratadns-gopher.svg?branch=master)](https://travis-ci.org/niclabs/ratadns-gopher)

RaTA DNS aggregator module. Service that process DNS packets information and sends it to a HTML5 SSE.

# Developer info
In order to start developing you must install the dependencies. Assuming that you have virtualenv:

```
$ git clone https://github.com/niclabs/ratadns-gopher
$ git checkout -b python
$ cd ratadns-gopher
$ virtualenv env
$ source env/bin/activate
(env) $ pip install -r requirements.txt
```
