// MOST Web Framework Codename Zero Gravity Copyright (c) 2017-2022, THEMOST LP All rights reserved
import { sprintf } from 'sprintf-js';
import { SqlFormatter } from '@themost/query';

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
        return sprintf('CAST(%s as SIGNED)', this.escape(expr));
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
}

export {
    MySqlFormatter,
    zeroPad
}