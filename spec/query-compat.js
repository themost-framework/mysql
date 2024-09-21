import { SqlFormatter, QueryExpression } from '@themost/query';

if (typeof SqlFormatter.prototype.formatInsert === 'function') {
    const superFormatInsert = SqlFormatter.prototype.formatInsert;
    if (/if\s+\(\w+\s+instanceof\s+QueryExpression\)/gm.test(superFormatInsert.toString()) === false) {
        SqlFormatter.prototype.formatInsert = function (expr) {
            if (expr && expr.$insert) {
                const [entity] = Object.keys(expr.$insert);
                if (entity) {
                    const innerExpr = expr.$insert[entity];
                    if (innerExpr instanceof QueryExpression) {
                        const sql = 'INSERT INTO';
                        sql += ' ';
                        sql += this.escapeEntity(entity);
                        sql += ' ';
                        sql += this.formatSelect(innerExpr);
                        return sql;
                    }
                }
            } 
            return superFormatInsert.call(this, expr);
        };
    }
}
