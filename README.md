# ratadns-gopher
RaTA DNS aggregator module.
Service that process DNS packets information and sends it to a HTML5 SSE.
The file `servData.go` process information of queries per second and answers per second.
The file `geo.go` adds geo-localization information to the packets that reads.
The file `sse.go` generates different windows of information to check the top n most request senders (or bad request senders) in a lapse of time (90 seconds, for example).
The file `main.go` is the one that starts every process for every HTML5 SSE URLs.
The directory servers has all the files that process the DNS packets, with the general structure to unmarshall/marshal the messages.
The directory sse has the structure that HTML5 requires to use an URL as a SSE.
The directory util has any file that are used among all the other files.

INSTALLATION
--------------------------
How to install:
    1. Install go.
    2. Install the required libraries with the following lines:
`go get gopkg.in/natefinch/lumberjack.v2`
`go get gopkg.in/redis.v3`

RUN
______________________
To run ratadns-gopher, after installing the required libraries, execute:
`go run main.go`