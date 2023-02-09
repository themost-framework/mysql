// MOST Web Framework Codename Zero Gravity Copyright (c) 2017-2022, THEMOST LP All rights reserved
import mysql from 'mysql2';
import async from 'async';
import { sprintf } from 'sprintf-js';
import { QueryExpression, QueryField } from '@themost/query';
import { TraceUtils } from '@themost/common';
import { MySqlFormatter, zeroPad } from './MySqlFormatter';

/**
 * @class
 * @constructor
 * @augments DataAdapter
 */

class MySqlAdapter {
    constructor(options) {
        /**
         * @private
         * @type {Connection}
         */
        this.rawConnection = null;
        /**
         * Gets or sets database connection string
         * @type {*}
         */
        this.options = options;
        /**
         * Gets or sets a boolean that indicates whether connection pooling is enabled or not.
         * @type {boolean}
         */
        this.connectionPooling = false;

    }

    /**
     * Opens database connection
     */
    open(callback) {
        callback = callback || function () { };
        const self = this;
        if (this.rawConnection) {
            return callback();
        }
        //get current timezone
        const offset = (new Date()).getTimezoneOffset(), timezone = (offset <= 0 ? '+' : '-') + zeroPad(-Math.floor(offset / 60), 2) + ':' + zeroPad(offset % 60, 2);
        if (self.connectionPooling) {
            if (typeof MySqlAdapter.pool === 'undefined') {
                MySqlAdapter.pool = mysql.createPool(this.options);
            }
            MySqlAdapter.pool.getConnection(function (err, connection) {
                if (err) {
                    return callback(err);
                }
                else {
                    self.rawConnection = connection;
                    self.execute('SET time_zone=?', timezone, function (err) {
                        return callback(err);
                    });
                }
            });
        }
        else {
            self.rawConnection = mysql.createConnection(this.options);
            self.rawConnection.connect(function (err) {
                if (err) {
                    return callback(err);
                }
                else {
                    //set connection timezone
                    self.execute('SET time_zone=?', timezone, function (err) {
                        return callback(err);
                    });
                }
            });
        }
    }

    openAsync() {
        return new Promise((resolve, reject) => {
            return this.open((err) => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    /**
     * @param {Function} callback
     */
    close(callback) {
        const self = this;
        callback = callback || function () { };
        if (!self.rawConnection)
            return;
        if (self.connectionPooling) {
            self.rawConnection.release();
            self.rawConnection = null;
        }
        else {
            self.rawConnection.end(function (err) {
                if (err) {
                    TraceUtils.log(err);
                    //do nothing
                    self.rawConnection = null;
                }
                callback();
            });
        }
    }

    closeAsync() {
        return new Promise((resolve, reject) => {
            return this.close((err) => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    /**
     * Begins a data transaction and executes the given function
     * @param {Function} fn
     * @param {Function} callback
     */
    executeInTransaction(fn, callback) {
        const self = this;
        //ensure callback
        callback = callback || function () { };
        //ensure that database connection is open
        self.open(function (err) {
            if (err) {
                return callback.bind(self)(err);
            }
            //execution is already in transaction
            if (self.__transaction) {
                //so invoke method
                fn.bind(self)(function (err) {
                    //call callback
                    callback.bind(self)(err);
                });
            }
            else {
                self.execute('START TRANSACTION', null, function (err) {
                    if (err) {
                        callback.bind(self)(err);
                    }
                    else {
                        //set transaction flag to true
                        self.__transaction = true;
                        try {
                            //invoke method
                            fn.bind(self)(function (error) {
                                if (error) {
                                    //rollback transaction
                                    self.execute('ROLLBACK', null, function () {
                                        //st flag to false
                                        self.__transaction = false;
                                        //call callback
                                        callback.bind(self)(error);
                                    });
                                }
                                else {
                                    //commit transaction
                                    self.execute('COMMIT', null, function (err) {
                                        //set flag to false
                                        self.__transaction = false;
                                        //call callback
                                        callback.bind(self)(err);
                                    });
                                }
                            });
                        }
                        catch (err) {
                            //rollback transaction
                            self.execute('ROLLBACK', null, function (err) {
                                //set flag to false
                                self.__transaction = false;
                                //call callback
                                callback.bind(self)(err);
                            });
                        }

                    }
                });
            }
        });
    }

    /**
     * Begins a data transaction and executes the given function
     * @param func {Function}
     */
     executeInTransactionAsync(func) {
        return new Promise((resolve, reject) => {
            return this.executeInTransaction((callback) => {
                return func.call(this).then(res => {
                    return callback(null, res);
                }).catch(err => {
                    return callback(err);
                });
            }, (err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve(res);
            });
        });
    }

    /**
     * Executes an operation against database and returns the results.
     * @param {DataModelBatch} batch
     * @param {Function} callback
     */
    executeBatch(batch, callback) {
        callback = callback || function () { };
        callback(new Error('DataAdapter.executeBatch() is obsolete. Use DataAdapter.executeInTransaction() instead.'));
    }

    /**
     * Produces a new identity value for the given entity and attribute.
     * @param {string} entity The target entity name
     * @param {string} attribute The target attribute
     * @param {Function=} callback
     */
    selectIdentity(entity, attribute, callback) {

        const self = this;

        const migration = {
            appliesTo: 'increment_id',
            model: 'increments',
            description: 'Increments migration (version 1.0)',
            version: '1.0',
            add: [
                { name: 'id', type: 'Counter', primary: true },
                { name: 'entity', type: 'Text', size: 120 },
                { name: 'attribute', type: 'Text', size: 120 },
                { name: 'value', type: 'Integer' }
            ]
        };
        //ensure increments entity
        self.migrate(migration, function (err) {
            //throw error if any
            if (err) { callback.bind(self)(err); return; }

            self.execute('SELECT * FROM `increment_id` WHERE `entity`=? AND `attribute`=?', [entity, attribute], function (err, result) {
                if (err) { callback.bind(self)(err); return; }
                if (result.length === 0) {
                    //get max value by querying the given entity
                    const q = new QueryExpression().from(entity).select([new QueryField().max(attribute)]);
                    self.execute(q, null, function (err, result) {
                        if (err) { callback.bind(self)(err); return; }
                        let value = 1;
                        if (result.length > 0) {
                            value = parseInt(result[0][attribute]) + 1;
                        }
                        self.execute('INSERT INTO `increment_id`(`entity`, `attribute`, `value`) VALUES (?,?,?)', [entity, attribute, value], function (err) {
                            //throw error if any
                            if (err) { callback.bind(self)(err); return; }
                            //return new increment value
                            callback.bind(self)(err, value);
                        });
                    });
                }
                else {
                    //get new increment value
                    const value = parseInt(result[0].value) + 1;
                    self.execute('UPDATE `increment_id` SET `value`=? WHERE `id`=?', [value, result[0].id], function (err) {
                        //throw error if any
                        if (err) { callback.bind(self)(err); return; }
                        //return new increment value
                        callback.bind(self)(err, value);
                    });
                }
            });
        });
    }

    /**
     * @param {string} entity 
     * @param {string} attribute 
     * @returns Promise<any>
     */
    selectIdentityAsync(entity, attribute) {
        return new Promise((resolve, reject) => {
            return this.selectIdentity(entity, attribute, (err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve(res);
            });
        });
    }

    /**
     * @param query {*}
     * @param values {*}
     * @param {function} callback
     */
    execute(query, values, callback) {
        const self = this;
        let sql = null;
        try {

            if (typeof query === 'string') {
                sql = query;
            }
            else {
                //format query expression or any object that may be act as query expression
                const formatter = new MySqlFormatter();
                formatter.settings.nameFormat = MySqlAdapter.NAME_FORMAT;
                sql = formatter.format(query);
            }
            //validate sql statement
            if (typeof sql !== 'string') {
                callback.bind(self)(new Error('The executing command is of the wrong type or empty.'));
                return;
            }
            //ensure connection
            self.open(function (err) {
                if (err) {
                    callback.bind(self)(err);
                }
                else {
                    let startTime;
                    if (process.env.NODE_ENV === 'development') {
                        startTime = new Date().getTime();
                    }
                    //execute raw command
                    self.rawConnection.query(sql, values, function (err, result) {
                        if (process.env.NODE_ENV === 'development') {
                            TraceUtils.log(sprintf('SQL (Execution Time:%sms):%s, Parameters:%s', (new Date()).getTime() - startTime, sql, JSON.stringify(values)));
                        }
                        callback.bind(self)(err, result);
                    });
                }
            });
        }
        catch (err) {
            callback.bind(self)(err);
        }
    }

    /**
     * @param {*} query
     * @param {*=} values
     * @returns Promise<void>
     */
     executeAsync(query, values) {
        return new Promise((resolve, reject) => {
            return this.execute(query, values, (err, results) => {
                if (err) {
                    return reject(err);
                }
                return resolve(results);
            });
        });
    }

    /**
     * Formats an object based on the format string provided. Valid formats are:
     * %t : Formats a field and returns field type definition
     * %f : Formats a field and returns field name
     * @param  {string} format
     * @param {*} obj
     */
    static format(format, obj) {
        let result = format;
        if (/%t/.test(format))
            result = result.replace(/%t/g, MySqlAdapter.formatType(obj));
        if (/%f/.test(format))
            result = result.replace(/%f/g, obj.name);
        return result;
    }

    static formatType(field) {
        const size = parseInt(field.size);
        const scale = parseInt(field.scale);
        let s = 'varchar(512) NULL';
        const type = field.type;
        switch (type) {
            case 'Boolean':
                s = 'tinyint(1)';
                break;
            case 'Byte':
                s = 'tinyint(3) unsigned';
                break;
            case 'Number':
            case 'Float':
                s = 'float';
                break;
            case 'Counter':
                return 'int(11) auto_increment not null';
            case 'Currency':
                s = 'decimal(19,4)';
                break;
            case 'Decimal':
                s = sprintf('decimal(%s,%s)', (size > 0 ? size : 19), (scale > 0 ? scale : 8));
                break;
            case 'Date':
                s = 'date';
                break;
            case 'DateTime':
            case 'Time':
                s = 'timestamp';
                break;
            case 'Integer':
                s = 'int(11)';
                break;
            case 'Duration':
                s = size > 0 ? sprintf('varchar(%s,0)', size) : 'varchar(36)';
                break;
            case 'URL':
            case 'Text':
                s = size > 0 ? `varchar(${size})` : 'varchar(512)';
                break;
            case 'Note':
                s = size > 0 ? `varchar(${size})` : 'text';
                break;
            case 'Image':
            case 'Binary':
                s = size > 0 ? `blob(${size})` : 'blob';
                break;
            case 'Guid':
                s = 'varchar(36)';
                break;
            case 'Short':
                s = 'smallint(6)';
                break;
            default:
                s = 'int(11)';
                break;
        }
        if (field.primary === true) {
            s += ' not null';
        }
        else {
            s += (typeof field.nullable === 'undefined') ? ' null' : ((field.nullable === true || field.nullable === 1) ? ' null' : ' not null');

        }
        return s;
    }

    /**
     * @param {string} name
     * @param {QueryExpression} query
     * @param {Function} callback
     */
    createView(name, query, callback) {
        this.view(name).create(query, callback);
    }

    /**
     *
     * @param  {MySqlAdapterMigration} obj - An Object that represents the data model scheme we want to migrate
     * @param {Function} callback
     */
    migrate(obj, callback) {
        if (obj === null)
            return;
        const self = this;
        const migration = obj;
        if (migration.appliesTo === null)
            throw new Error('Model name is undefined');
        self.open(function (err) {
            if (err) {
                callback.bind(self)(err);
            }
            else {
                async.waterfall([
                    //1. Check migrations table existence
                    function (cb) {
                        self.table('migrations').exists(function (err, exists) {
                            if (err) { return cb(err); }
                            cb(null, exists);
                        });
                    },
                    //2. Create migrations table if not exists
                    function (arg, cb) {
                        if (arg > 0) { return cb(null, 0); }
                        self.table('migrations').create([
                            { name: 'id', type: 'Counter', primary: true, nullable: false },
                            { name: 'appliesTo', type: 'Text', size: '80', nullable: false },
                            { name: 'model', type: 'Text', size: '120', nullable: true },
                            { name: 'description', type: 'Text', size: '512', nullable: true },
                            { name: 'version', type: 'Text', size: '40', nullable: false }
                        ], function (err) {
                            if (err) { return cb(err); }
                            cb(null, 0);
                        });
                    },
                    //3. Check if migration has already been applied
                    function (arg, cb) {
                        self.execute('SELECT COUNT(*) AS `count` FROM `migrations` WHERE `appliesTo`=? and `version`=?',
                            [migration.appliesTo, migration.version], function (err, result) {
                                if (err) {
                                    return cb(err);
                                }
                                return cb(null, result[0].count);
                            });
                    },
                    //4a. Check table existence
                    function (arg, cb) {
                        //migration has already been applied (set migration.updated=true)
                        if (arg > 0) {
                            obj.updated = true;
                            return cb(null, -1);
                        }
                        self.table(migration.appliesTo).exists(function (err, exists) {
                            if (err) {
                                return cb(err);
                            }
                            return cb(null, exists ? -1 : 0);
                        });
                    },
                    //4b. Migrate target table (create or alter)
                    function (arg, cb) {
                        //migration has already been applied
                        if (arg < 0) {
                            return cb(null, arg);
                        }
                        if (arg === 0) {
                            //create table
                            return self.table(migration.appliesTo).create(migration.add, function (err) {
                                if (err) {
                                    return cb(err);
                                }
                                return cb(null, 1);
                            });
                        }
                        //columns to be removed (unsupported)
                        if (Array.isArray(migration.remove)) {
                            if (migration.remove.length > 0) {
                                return cb(new Error('Data migration remove operation is not supported by this adapter.'));
                            }
                        }
                        //columns to be changed (unsupported)
                        if (Array.isArray(migration.change)) {
                            if (migration.change.length > 0) {
                                return cb(new Error('Data migration change operation is not supported by this adapter. Use add collection instead.'));
                            }
                        }
                        let column, newType, oldType;
                        if (Array.isArray(migration.add)) {
                            //init change collection
                            migration.change = [];
                            //get table columns
                            self.table(migration.appliesTo).columns(function (err, columns) {
                                if (err) { return cb(err); }
                                for (let i = 0; i < migration.add.length; i++) {
                                    const x = migration.add[i];
                                    column = columns.find(function (y) { return (y.name === x.name); });
                                    if (column) {
                                        //if column is primary key remove it from collection
                                        if (column.primary) {
                                            migration.add.splice(i, 1);
                                            i -= 1;
                                        }
                                        else {
                                            //get new type
                                            newType = MySqlAdapter.format('%t', x);
                                            //get old type
                                            oldType = column.type1.replace(/\s+$/, '') + ((column.nullable === true || column.nullable === 1) ? ' null' : ' not null');
                                            //remove column from collection
                                            migration.add.splice(i, 1);
                                            i -= 1;
                                            if (newType !== oldType) {
                                                //add column to alter collection
                                                migration.change.push(x);
                                            }
                                        }
                                    }
                                }
                                //alter table
                                const targetTable = self.table(migration.appliesTo);
                                //add new columns (if any)
                                targetTable.add(migration.add, function (err) {
                                    if (err) { return cb(err); }
                                    //modify columns (if any)
                                    targetTable.change(migration.change, function (err) {
                                        if (err) { return cb(err); }
                                        cb(null, 1);
                                    });
                                });
                            });
                        }
                        else {
                            cb(new Error('Invalid migration data.'));
                        }
                    },
                    //Apply data model indexes
                    function (arg, cb) {
                        if (arg <= 0) { return cb(null, arg); }
                        if (migration.indexes) {
                            const tableIndexes = self.indexes(migration.appliesTo);
                            //enumerate migration constraints
                            async.eachSeries(migration.indexes, function (index, indexCallback) {
                                tableIndexes.create(index.name, index.columns, indexCallback);
                            }, function (err) {
                                //throw error
                                if (err) { return cb(err); }
                                //or return success flag
                                return cb(null, 1);
                            });
                        }
                        else {
                            //do nothing and exit
                            return cb(null, 1);
                        }
                    },
                    function (arg, cb) {
                        if (arg > 0) {
                            //log migration to database
                            self.execute('INSERT INTO `migrations` (`appliesTo`,`model`,`version`,`description`) VALUES (?,?,?,?)', [migration.appliesTo,
                            migration.model,
                            migration.version,
                            migration.description], function (err) {
                                if (err) { return cb(err); }
                                return cb(null, 1);
                            });
                        }

                        else
                            cb(null, arg);

                    }
                ], function (err, result) {
                    callback(err, result);
                });
            }
        });
    }

    /**
     * @param {*} obj
     * @returns Promise<*>
     */
     migrateAsync(obj) {
        return new Promise((resolve, reject) => {
            return this.migrate(obj, (err, result) => {
                if (err) {
                    return reject(err);
                }
                return resolve(result);
            });
        });
    }

    table(name) {
        const self = this;

        return {
            /**
             * @param {function(Error,Boolean=)} callback
             */
            exists: function (callback) {
                callback = callback || function () { };
                self.execute('SELECT COUNT(*) AS `count` FROM `information_schema`.`TABLES` WHERE `TABLE_NAME`=? AND `TABLE_SCHEMA`=DATABASE()',
                    [
                        name
                    ], function (err, result) {
                        if (err) { 
                            return callback(err);
                        }
                        return callback(null, result[0].count > 0);
                    });
            },
            existsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.exists((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            /**
             * @param {function(Error,string=)} callback
             */
            version: function (callback) {
                callback = callback || function () { };
                self.execute('SELECT MAX(`version`) AS `version` FROM `migrations` WHERE `appliesTo`=?',
                    [name], function (err, result) {
                        if (err) { return callback(err); }
                        if (result.length === 0)
                            callback(null, '0.0');

                        else
                            callback(null, result[0].version || '0.0');
                    });
            },
            versionAsync: function () {
                return new Promise((resolve, reject) => {
                    this.version((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            /**
             * @param {function(Error=,Array=)} callback
             */
            columns: function (callback) {
                callback = callback || function () { };
                self.execute('SELECT COLUMN_NAME AS `name`, DATA_TYPE as `type`, ' +
                    'CHARACTER_MAXIMUM_LENGTH as `size`,CASE WHEN IS_NULLABLE=\'YES\' THEN 1 ELSE 0 END AS `nullable`, ' +
                    'NUMERIC_PRECISION as `precision`, NUMERIC_SCALE as `scale`, ' +
                    'CASE WHEN COLUMN_KEY=\'PRI\' THEN 1 ELSE 0 END AS `primary`, ' +
                    'CONCAT(COLUMN_TYPE, (CASE WHEN EXTRA = NULL THEN \'\' ELSE CONCAT(\' \',EXTRA) END)) AS `type1` ' +
                    'FROM information_schema.COLUMNS WHERE TABLE_NAME=? AND TABLE_SCHEMA=DATABASE()',
                    [name], function (err, result) {
                        if (err) {
                            return callback(err);
                        }
                        return callback(null, result);
                    });
            },
            columnsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.columns((err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * @param {Array} fields
             * @param {Function} callback
             */
            create: function (fields, callback) {
                callback = callback || function () { };
                fields = fields || [];
                if (Array.isArray(fields) === false) {
                    return callback(new Error('Invalid argument type. Expected Array.'));
                }
                if (fields.length === 0) {
                    return callback(new Error('Invalid argument. Fields collection cannot be empty.'));
                }
                let strFields = fields.filter((x) => { return !x.oneToMany; }).map(
                    (x) => {
                        return MySqlAdapter.format('`%f` %t', x);
                    }).join(', ');
                //add primary key constraint
                const strPKFields = fields.filter((x) => {
                    return (x.primary === true || x.primary === 1);
                }).map((x) => {
                    return MySqlAdapter.format('`%f`', x);
                }).join(', ');
                if (strPKFields.length > 0) {
                    strFields += ', ' + sprintf('PRIMARY KEY (%s)', strPKFields);
                }
                const sql = sprintf('CREATE TABLE %s (%s)', name, strFields);
                self.execute(sql, null, function (err) {
                    callback(err);
                });
            },
            createAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.create(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * Alters the table by adding an array of fields
             * @param {{name:string,type:string,primary:boolean|number,nullable:boolean|number,size:number,oneToMany:boolean}[]|*} fields
             * @param callback
             */
            add: function (fields, callback) {
                callback = callback || function () { };
                fields = fields || [];
                if (Array.isArray(fields) === false) {
                    //invalid argument exception
                    return callback(new Error('Invalid argument type. Expected Array.'));
                }
                if (fields.length === 0) {
                    //do nothing
                    return callback();
                }
                const formatter = new MySqlFormatter();
                const strTable = formatter.escapeName(name);
                const statements = fields.map(function (x) {
                    return MySqlAdapter.format('ALTER TABLE ' + strTable + ' ADD COLUMN `%f` %t', x);
                });
                return async.eachSeries(statements, function (sql, cb) {
                    self.execute(sql, [], function (err) {
                        return cb(err);
                    });
                }, function (err) {
                    return callback(err);
                });
            },
            addAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.add(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * Alters the table by modifying an array of fields
             * @param {{name:string,type:string,primary:boolean|number,nullable:boolean|number,size:number,oneToMany:boolean}[]|*} fields
             * @param callback
             */
            change: function (fields, callback) {
                callback = callback || function () { };
                fields = fields || [];
                if (Array.isArray(fields) === false) {
                    //invalid argument exception
                    return callback(new Error('Invalid argument type. Expected Array.'));
                }
                if (fields.length === 0) {
                    //do nothing
                    return callback();
                }
                const formatter = new MySqlFormatter();
                const strTable = formatter.escapeName(name);
                const statements = fields.map(function (x) {
                    return MySqlAdapter.format('ALTER TABLE ' + strTable + ' MODIFY COLUMN `%f` %t', x);
                });
                return async.eachSeries(statements, function (sql, cb) {
                    self.execute(sql, [], function (err) {
                        return cb(err);
                    });
                }, function (err) {
                    return callback(err);
                });
            },
            changeAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.change(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            }
        };
    }

    view(name) {
        const self = this;
        let owner;
        let view = name;
        const matches = /(\w+)\.(\w+)/.exec(name);
        if (matches) {
            //get schema owner
            // eslint-disable-next-line no-unused-vars
            owner = matches[1];
            //get table name
            view = matches[2];
        }
        else {
            view = name;
        }
        return {
            /**
             * @param {function(Error,Boolean=)} callback
             */
            exists: function (callback) {
                const sql = 'SELECT COUNT(*) AS `count` FROM information_schema.TABLES WHERE TABLE_NAME=? AND TABLE_TYPE=\'VIEW\' AND TABLE_SCHEMA=DATABASE()';
                self.execute(sql, [
                    view
                ], function (err, result) {
                    if (err) { callback(err); return; }
                    callback(null, (result[0].count > 0));
                });
            },
            existsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.exists((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            /**
             * @param {Function} callback
             */
            drop: function (callback) {
                callback = callback || function () { };
                self.open((err) => {
                    if (err) {
                        return callback(err);
                    }
                    const sql = 'SELECT COUNT(*) AS `count` FROM information_schema.TABLES WHERE TABLE_NAME=? AND TABLE_TYPE=\'VIEW\' AND TABLE_SCHEMA=DATABASE()';
                    self.execute(sql, [
                        view
                    ], (err, result) => {
                        if (err) {
                            return callback(err);
                        }
                        const exists = (result[0].count > 0);
                        if (exists) {
                            const formatter = new MySqlFormatter();
                            const sql = sprintf('DROP VIEW %s', formatter.escapeName(name));
                            return self.execute(sql, [], function (err) {
                                if (err) {
                                    return callback(err);
                                }
                                return callback();
                            });
                        }
                        return callback();
                    });
                });
            },
            dropAsync: function () {
                return new Promise((resolve, reject) => {
                    this.drop((err) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve();
                    });
                });
            },
            /**
             * @param {QueryExpression|*} q
             * @param {Function} callback
             */
            create: function (q, callback) {
                self.executeInTransaction((tr) => {
                    this.drop((err) => {
                        if (err) {
                            return tr(err); 
                        }
                        try {
                            const formatter = new MySqlFormatter();
                            let sql = sprintf('CREATE VIEW %s AS ', formatter.escapeName(name));
                            sql += formatter.format(q);
                            self.execute(sql, [], tr);
                        }
                        catch (e) {
                            return tr(e);
                        }
                    });
                }, (err) => {
                    return callback(err);
                });
            },
            createAsync: function (q) {
                return new Promise((resolve, reject) => {
                    this.create(q, (err) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve();
                    });
                });
            }
        };
    }

    indexes(table) {
        const self = this, formatter = new MySqlFormatter();
        return {
            list: function (callback) {
                const this1 = this;
                if (Object.prototype.hasOwnProperty.call(this1, 'indexes_')) {
                    return callback(null, this1['indexes_']);
                }
                self.execute(sprintf('SHOW INDEXES FROM `%s`', table), null, function (err, result) {
                    if (err) { return callback(err); }
                    const indexes = [];
                    result.forEach(function (x) {
                        const obj = indexes.find(function (y) { return y.name === x['Key_name']; });
                        if (typeof obj === 'undefined') {
                            indexes.push({
                                name: x['Key_name'],
                                columns: [x['Column_name']]
                            });
                        }
                        else {
                            obj.columns.push(x['Column_name']);
                        }
                    });
                    return callback(null, indexes);
                });
            },
            listAsync: function () {
                return new Promise((resolve, reject) => {
                    this.list((err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * @param {string} name
             * @param {Array|string} columns
             * @param {Function} callback
             */
            create: function (name, columns, callback) {
                const cols = [];
                if (typeof columns === 'string') {
                    cols.push(columns);
                }
                else if (Array.isArray(columns)) {
                    cols.push.apply(cols, columns);
                }
                else {
                    return callback(new Error('Invalid parameter. Columns parameter must be a string or an array of strings.'));
                }
                const thisArg = this;
                thisArg.list(function (err, indexes) {

                    if (err) { return callback(err); }
                    const ix = indexes.find(function (x) { return x.name === name; });
                    //format create index SQL statement
                    const sqlCreateIndex = sprintf('CREATE INDEX %s ON %s(%s)',
                        formatter.escapeName(name),
                        formatter.escapeName(table),
                        cols.map(function (x) {
                            return formatter.escapeName(x);
                        }).join(','));
                    if (typeof ix === 'undefined' || ix === null) {
                        self.execute(sqlCreateIndex, [], callback);
                    }
                    else {
                        let nCols = cols.length;
                        //enumerate existing columns
                        ix.columns.forEach(function (x) {
                            if (cols.indexOf(x) >= 0) {
                                //column exists in index
                                nCols -= 1;
                            }
                        });
                        if (nCols > 0) {
                            //drop index
                            thisArg.drop(name, function (err) {
                                if (err) { return callback(err); }
                                //and create it
                                self.execute(sqlCreateIndex, [], callback);
                            });
                        }
                        else {
                            //do nothing
                            return callback();
                        }
                    }
                });
            },
            /**
             * @param {string} name
             * @param {Array|string} columns
             */
             createAsync: function (name, columns) {
                return new Promise((resolve, reject) => {
                    this.create(name, columns, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            drop: function (name, callback) {
                if (typeof name !== 'string') {
                    return callback(new Error('Name must be a valid string.'));
                }
                this.list((err, indexes) => {
                    if (err) {
                        return callback(err);
                    }
                    const exists = indexes.find(function (x) { return x.name === name; }) != null;
                    if (!exists) {
                        return callback();
                    }
                    self.execute(sprintf('DROP INDEX %s ON %s', formatter.escapeName(name), formatter.escapeName(table)), [], callback);
                });
            },
            dropAsync: function (name) {
                return new Promise((resolve, reject) => {
                    this.drop(name, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            }
        };
    }

    queryFormat(query, values) {
        if (!values)
            return query;
        const self = this;
        return query.replace(/:(\w+)/g, function (txt, key) {
            if (Object.prototype.hasOwnProperty.call(values, key)) {
                return self.escape(values[key]);
            }
            return txt;
        }.bind(this));
    }

    /**
     * Database helper
     * @param {string} name - A string that represents the database name
     * @returns {*}
     */
     database(name) {
        const self = this;
        return {
            exists: function (callback) {
                return self.execute('SHOW DATABASES;', [], (err, results) => {
                    if (err) {
                        return callback(err);
                    }
                    const exists = results.findIndex((x) => x.Database === name);
                    return callback(null, exists >= 0);
                });
            },
            existsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.exists((err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            create: function (callback) {
                return self.execute(`CREATE DATABASE ${self.escapeName(name)};`, [], (err) => {
                    if (err) {
                        return callback(err);
                    }
                    return callback();
                });
            },
            createAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.create(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            }
        }
     }
}

export {
    MySqlAdapter
}