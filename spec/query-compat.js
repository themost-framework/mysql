import { QueryExpression } from '@themost/query';

if (typeof QueryExpression.prototype.formatInsert === 'function') {
    const superFormatInsert = QueryExpression.prototype.formatInsert;
    if (/if\s+\(\w+\s+instanceof\s+QueryExpression\)/gm.test(superFormatInsert.toString()) === false) {
        QueryExpression.prototype.formatInsert = function (expr) {
            if (expr.$insert) {
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
