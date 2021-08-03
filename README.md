# InfluxDB line-protocol codec

This module implements a high performance Go codec for the line-protocol syntax as accepted by InfluxDB.
Currently the API is low level - it's intended for converting line-protocol to some chosen concrete
types that aren't specified here. (In future work, we'll define a `Point` type that implements a convenient
but less performant type to encode or decode).

The API documentation is here: https://pkg.go.dev/github.com/influxdata/line-protocol/v2/lineprotocol
