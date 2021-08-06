# go-queue

Go queue provides a unified API across a variety of different queue backend like Redis, in-memory sync/async and RabbitMQ.

<p align="left">
    <a href="https://github.com/ibllex/go-queue/actions/workflows/test.yml" title="Test Status">
        <img src="https://github.com/ibllex/go-queue/actions/workflows/test.yml/badge.svg">
    </a>
    <a href="https://github.com/ibllex/go-queue/issues" title="issues">
        <img src="https://img.shields.io/github/issues/ibllex/go-queue">
    </a>
    <a href="https://github.com/ibllex/go-queue" title="stars">
        <img src="https://img.shields.io/github/stars/ibllex/go-queue">
    </a>
    <a href="https://github.com/ibllex/go-queue" title="forks">
        <img src="https://img.shields.io/github/forks/ibllex/go-queue">
    </a>
    <a href="./LICENSE" title="license">
        <img src="https://img.shields.io/github/license/ibllex/go-queue">
    </a>
</p>

## Features

- RabbitMQ, in-memory sync/async backends.
- Multiple message codecs (json, gob and msgpack) supported by [go-encoding](https://github.com/ibllex/go-encoding), you can also implement your own codecs.
- Automatic message encoding and compression.
- Support publishing raw messages for easy use with other languages.
- Support for asynchronous task management and distribution.

## Install

```bash
go get -u github.com/ibllex/go-queue
```

## Qucik Usage

For basic asynchronous message useage please see the [example/raw](./example/raw/main.go)

For asynchronous task useage please see the [example/task](./example/task/main.go)

## License

This library is under the [BSD-2](./LICENSE) license.
