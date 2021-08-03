# go-queue

Go queue provides a unified API across a variety of different queue backend like Redis, in-memory and RabbitMQ.

## Features

- RabbitMQ, in-memory backends.
- Multiple message codecs (json, gob and msgpack) supported by [go-encoding](https://github.com/ibllex/go-encoding), you can also implement your own codecs.
- Automatic message encoding and compression.
- Support publishing raw messages for easy use with other languages.

## Install

```bash
go get -u github.com/ibllex/go-queue
```

## Qucik Usage

Please see the [example](./example/main.go)

## License

This library is under the [BSD-2](./LICENSE) license.
