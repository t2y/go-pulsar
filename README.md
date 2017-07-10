# go-pulsar

go-pulsar is a [pulsar](https://github.com/apache/incubator-pulsar) client library.

* [Pulsar Documentation](https://github.com/apache/incubator-pulsar/blob/master/docs/Documentation.md)


## Development Status

**Alpha.**

go-pulsar is still under heavy development. Some functionality are known to be broken, missing or incomplete. The interface may also change.

### TODO

* partitioned topics functions
* payload compression
* unimplemented commands
* error handling


## How to build

### pulsar-client

pulsar-client is a cli tool to use go-pulsar library. It's like Java's [Pulsar client tool](https://github.com/apache/incubator-pulsar/blob/master/docs/AdminTools.md#pulsar-client-tool).

```bash
$ mkdir work && cd work
$ export GOPATH="$(pwd)"
$ mkdir -p src/github.com/t2y/go-pulsar
$ cd src/github.com/t2y/go-pulsar
$ git clone git@github.com:t2y/go-pulsar.git .
```

Get dependency libraries and build it.

```bash
$ make install-glide
$ make deps
$ make
$ ./bin/pulsar-client --help
Usage:
  pulsar-client [OPTIONS]

Application Options:
      --serviceUrl=                 pulsar service url [$PULSAR_SERVICE_URL]
      --authMethod=                 authentication method [$PULSAR_AUTH_METHOD]
      --authParams=                 authentication params [$PULSAR_AUTH_PARAMS]
      --useTls                      use tls to connect [$USE_TLS]
      --tlsAllowInsecureConnection  allow insecure tls connection [$TLS_ALLOW_INSECURE_CONNECTION]
      --athenzConf=                 path to athenz config file [$PULSAR_ATHENZ_CONF]
      --athenzAuthHeader=           athenz authentication header [$PULSAR_ATHENZ_AUTH_HEADER]
      --conf=                       path to pulsar config file [$PULSAR_CONF]
      --verbose                     use verbose mode [$VERBOSE]
      --timeout=                    timeout to communicate with pulsar broker [$PULSAR_TIMEOUT]
      --command=                    produce or consume [$PULSAR_COMMAND]
      --topic=                      topic name [$PULSAR_TOPIC]
      --messages=                   messages to produce [$PULSAR_MESSAGES]
      --properties=                 properties to produce. e.g) key1:value1,key2:value2 [$PULSAR_PROPERTIES]
      --numMessages=                number of messages to consume (default: 1) [$PULSAR_NUM_MESSAGES]
      --subscriptionName=           subscription name [$PULSAR_SUBSCRIPTION_NAME]
      --subscriptionType=           subscription type: exclusive, shared, failover (default: exclusive) [$PULSAR_SUBSCRIPTION_TYPE]

Help Options:
  -h, --help                        Show this help message
```

Some options can be set by *ini* file. There're a sample file in *example* directory.

```bash
$ cat example/default.ini
log_level = info
service_url = pulsar://localhost:6650/
timeout = 40s
min_connection_num = 2
max_connection_num = 20
```

### Code generation for pulsar protocol

pulsar protocol is defined using [Protocol Buffers](https://developers.google.com/protocol-buffers/).

* [Pulsar binary protocol specification](https://github.com/apache/incubator-pulsar/blob/master/docs/BinaryProtocol.md)

go-pulsar also uses [PulsarApi.proto](https://github.com/apache/incubator-pulsar/blob/master/pulsar-common/src/main/proto/PulsarApi.proto) and generates go source code: [PulsarApi.pb.go](https://github.com/t2y/go-pulsar/blob/master/proto/pb/PulsarApi.pb.go).

First of all, install *protoc* command for your platform. For example, use [Homebrew](https://brew.sh/) on macOS.

```bash
$ brew install protobuf
```

To get latest `proto` file and re-generate go source code, make as follows.

```bash
$ make install-pb
$ make gen-pb
```

The following files are updated

* proto/PulsarApi.proto
* proto/pb/PulsarApi.pb.go


## Getting started

### Startup Pulsar server

Build and install pulsar server.

```bash
$ git clone https://github.com/apache/incubator-pulsar.git
$ cd pulsar
$ mvn install -DskipTests
```

Start up a standalone pulsar server for development.

```bash
$ ./bin/pulsar standalone
```

* [Getting started with Pulsar](https://github.com/apache/incubator-pulsar/blob/master/docs/GettingStarted.md)

### Producer

```bash
$ ./bin/pulsar-client --conf example/default.ini --command produce --topic "persistent://sample/standalone/ns1/my-topic" --messages "Hello Pulsar"
INFO[2017-06-15T08:43:47.887709192+09:00] read and parse ini file                       iniConf=&{info pulsar://localhost:6650/ 40s 2 20 pulsar://localhost:6650/ info} path="example/default.ini"
INFO[2017-06-15T08:43:49.222165203+09:00] messages successfully produced                messages=[Hello Pulsar] properties=[]
```

The `--verbose` option makes debug easy. It shows communications between producers/consumers and brokers.

### Consumer

```bash
$ ./bin/pulsar-client --conf example/default.ini --command consume --topic "persistent://sample/standalone/ns1/my-topic" --subscriptionName sub
INFO[2017-06-15T08:50:33.467806336+09:00] read and parse ini file                       iniConf=&{info pulsar://localhost:6650/ 40s 2 20 pulsar://localhost:6650/ info} path="example/default.ini"
INFO[2017-06-15T08:50:34.515306354+09:00] messages successfully consumed                key-value=[] message="Hello Pulsar"
```


## Authentication

go-pulsar supports [Athenz](https://github.com/yahoo/athenz/) authentication.

* [Pulsar Authentication: Athenz](https://github.com/apache/incubator-pulsar/blob/master/docs/Authentication.md#athenz)

Read above documentation for each athenz parameters. Then, you can set some or all parameters on your needs into *ini* file. There're a sample file for athenz in *example* directory.

```bash
$ cat example/athenz.ini 
log_level = info
service_url = pulsar://localhost:6650/
timeout = 40s
min_connection_num = 2
max_connection_num = 20

auth_method=athenz
auth_params=tenantDomain:${yourDomain},tenantService:${yourService},providerDomain:${yourProviderDomain},privateKeyPath:${pathToPrivateKeyFile},keyId:0
use_tls=true
tls_allow_insecure_connection=false
tls_trust_certs_filepath=${pathToRootCaCertsFileIfNeeded}

athenz_conf=${pathToAthenzConfFile}
athenz_auth_header=${yourAthenzAuthenticationHeader}
```

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
