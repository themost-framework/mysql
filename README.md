[![npm](https://img.shields.io/npm/v/@themost%2Fmysql.svg)](https://www.npmjs.com/package/@themost%2Fmysql)
![Dependency status for latest release](https://img.shields.io/librariesio/release/npm/@themost/mysql)
![GitHub top language](https://img.shields.io/github/languages/top/themost-framework/mysql)
[![License](https://img.shields.io/npm/l/@themost/mysql)](https://github.com/themost-framework/themost/blob/master/LICENSE)
![GitHub last commit](https://img.shields.io/github/last-commit/themost-framework/mysql)
![GitHub Release Date](https://img.shields.io/github/release-date/themost-framework/mysql)
[![npm](https://img.shields.io/npm/dw/@themost/mysql)](https://www.npmjs.com/package/@themost%2Fmysql)

![MOST Web Framework Logo](https://github.com/themost-framework/common/raw/master/docs/img/themost_framework_v3_128.png)

@themost/mysql
===========

Most Web Framework MySQL Adapter

License: [BSD-3-Clause](https://github.com/themost-framework/mysql/blob/master/LICENSE)

## Install
    npm install @themost/mysql
## Usage
Register MySQL adapter on app.json as follows:

    "adapterTypes": [
        ...
        { "name":"MySQL Data Adapter", "invariantName": "mysql", "type":"@themost/mysql" }
        ...
    ],
    adapters: [
        ...
        { "name":"development", "invariantName":"mysql", "default":true,
            "options": {
              "host":"localhost",
              "port":3306,
              "user":"user",
              "password":"password",
              "database":"test"
            }
        }
        ...
    ]
