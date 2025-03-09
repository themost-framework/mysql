// MOST Web Framework Codename Zero Gravity Copyright (c) 2017-2022, THEMOST LP All rights reserved
import { sprintf } from 'sprintf-js';
import { SqlFormatter, QueryField } from '@themost/query';
import { isObjectDeep } from './isObjectDeep';

function zeroPad(number, length) {
    number = number || 0;
    let res = number.toString();
    while (res.length < length) {
        res = '0' + res;
    }
    return res;
}

class MySqlFormatter extends SqlFormatter {
    /**
     * @constructor
     */
    constructor() {
        super();
        Object.assign(this.settings, {
            nameFormat: '`$1`',
            forceAlias: true,
            useAliasKeyword: true
        });
    }

    escape(value, unquoted) {

        if (typeof value === 'boolean') { return value ? '1' : '0'; }
        if (Array.isArray(value)) {
            // find first non-object value
            const index = value.filter((x) => {
                return x != null;
            }).findIndex((x) => {
                return isObjectDeep(x) === false;
            });
            // if all values are objects
            if (index === -1) {
                return this.escape(JSON.stringify(value)); // return as json array
            }
        }
        if (value instanceof Date) {
            return this.escapeDate(value);
        }
        return super.escape.bind(this)(value, unquoted);
    }

    /**
     * @param {Date|*} val
     * @returns {string}
     */
    escapeDate(val) {
        const year = val.getFullYear();
        const month = zeroPad(val.getMonth() + 1, 2);
        const day = zeroPad(val.getDate(), 2);
        const hour = zeroPad(val.getHours(), 2);
        const minute = zeroPad(val.getMinutes(), 2);
        const second = zeroPad(val.getSeconds(), 2);
        //var millisecond = zeroPad(val.getMilliseconds(), 3);
        //format timezone
        const offset = val.getTimezoneOffset(), timezone = (offset <= 0 ? '+' : '-') + zeroPad(-Math.floor(offset / 60), 2) + ':' + zeroPad(offset % 60, 2);
        const datetime = year + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second;
        //convert timestamp to mysql server timezone (by using date object timezone offset)
        return sprintf('CONVERT_TZ(\'%s\',\'%s\', @@session.time_zone)', datetime, timezone);
    }

    $toString(expr) {
        return sprintf('CAST(%s as NCHAR)', this.escape(expr));
    }

    $toInt(expr) {
        return sprintf('FLOOR(CAST(%s as DECIMAL(19,8)))', this.escape(expr));
    }

    $toDouble(expr) {
        return this.$toDecimal(expr, 19, 8);
    }

    // noinspection JSCheckFunctionSignatures
    /**
     * @param {*} expr 
     * @param {number=} precision 
     * @param {number=} scale 
     * @returns 
     */
    $toDecimal(expr, precision, scale) {
        const p = typeof precision === 'number' ? Math.floor(precision) : 19;
        const s = typeof scale === 'number' ? Math.floor(scale) : 8;
        return sprintf('CAST(%s as DECIMAL(%s,%s))', this.escape(expr), p, s);
    }

    $toLong(expr) {
        return sprintf('CAST(%s as SIGNED)', this.escape(expr));
    }

    $uuid() {
        return 'UUID()';
    }

    $toGuid(expr) {
        return sprintf('BIN_TO_UUID(UNHEX(MD5(%s)))', this.escape(expr));
    }

    /**
     * 
     * @param {('date'|'datetime'|'timestamp')} type 
     * @returns 
     */
    $getDate(type) {
        switch (type) {
            case 'date':
                return 'CURRENT_DATE()';
            case 'datetime':
            case 'timestamp':
                return 'CURRENT_TIMESTAMP()';
            default:
                return 'CURRENT_TIMESTAMP()';
        }
    }

    /**
     * @param {*} expr
     * @return {string}
     */
    $jsonGet(expr) {
        if (typeof expr.$name !== 'string') {
            throw new Error('Invalid json expression. Expected a string');
        }
        const parts = expr.$name.split('.');
        const extract = this.escapeName(parts.splice(0, 2).join('.'));
        return `json_extract(${extract}, '$.${parts.join('.')}')`;
    }

    /**
     * @param {{ $jsonGet: Array<*> }} expr
     */
    $jsonGroupArray(expr) {
        const [key] = Object.keys(expr);
        if (key !== '$jsonObject') {
            throw new Error('Invalid json group array expression. Expected a json object expression');
        }
        return `JSON_ARRAYAGG(${this.escape(expr)})`;
    }

    /**
     * @param {import('@themost/query').QueryExpression} expr
     */
    $jsonArray(expr) {
        if (expr == null) {
            throw new Error('The given query expression cannot be null');
        }
        if (expr instanceof QueryField) {
            // escape expr as field and waiting for parsing results as json array
            return this.escape(expr);
        }
        // trear expr as select expression
        if (expr.$select) {
            // get select fields
            const args = Object.keys(expr.$select).reduce((previous, key) => {
                previous.push.apply(previous, expr.$select[key]);
                return previous;
            }, []);
            const [key] = Object.keys(expr.$select);
            // prepare select expression to return json array   
            expr.$select[key] = [
                {
                    $jsonGroupArray: [ // use json_group_array function
                        {
                            $jsonObject: args // use json_object function
                        }
                    ]
                }
            ];
            return `(${this.format(expr)})`;
        }
        // treat expression as query field
        if (Object.prototype.hasOwnProperty.call(expr, '$value')) {
            if (Array.isArray(expr.$value)) {
                const values = expr.$value.map((x) => {
                    return this.escape(x);
                }).join(',');
                return `JSON_ARRAY(${values})`;
            }
            return this.escape(expr);
        }
        if (Object.prototype.hasOwnProperty.call(expr, '$literal')) {
            if (Array.isArray(expr.$literal)) {
                const values = expr.$literal.map((x) => {
                    return this.escape(x);
                }).join(',');
                return `JSON_ARRAY(${values})`;
            }
            return this.escape(expr);
        }
        throw new Error('Invalid json array expression. Expected a valid select expression');
    }

    /**
     * @param {...*} expr
     */
    // eslint-disable-next-line no-unused-vars
    $json(expr) {
        const args = Array.from(arguments);
        return this.$jsonObject(...args);
    }

    /**
     * @param {...*} expr
     */
    // eslint-disable-next-line no-unused-vars
    $jsonObject(expr) {
        // expected an array of QueryField objects
        const args = Array.from(arguments).reduce((previous, current) => {
            // get the first key of the current object
            let [name] = Object.keys(current);
            let value;
            // if the name is not a string then throw an error
            if (typeof name !== 'string') {
                throw new Error('Invalid json object expression. The attribute name cannot be determined.');
            }
            // if the given name is a dialect function (starts with $) then use the current value as is
            // otherwise create a new QueryField object
            if (name.startsWith('$')) {
                value = new QueryField(current[name]);
                name = value.getName();
            } else {
                value = current instanceof QueryField ? new QueryField(current[name]) : current[name];
            }
            // escape json attribute name and value
            previous.push(this.escape(name), this.escape(value));
            return previous;
        }, []);
        return `JSON_OBJECT(${args.join(',')})`;
    }
}

export {
    MySqlFormatter,
    zeroPad
}