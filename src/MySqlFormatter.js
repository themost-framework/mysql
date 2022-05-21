// MOST Web Framework Codename Zero Gravity Copyright (c) 2017-2022, THEMOST LP All rights reserved
import util from 'util';
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
        this.settings = {
            nameFormat: '`$1`',
            forceAlias: true
        };
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
        return util.format('CONVERT_TZ(\'%s\',\'%s\', @@session.time_zone)', datetime, timezone);
    }
}

export {
    MySqlFormatter,
    zeroPad
}