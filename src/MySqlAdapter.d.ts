import { DataAdapterBase, DataAdapterBaseHelper, DataAdapterIndexes, DataAdapterTable, DataAdapterView, DataAdapterMigration } from '@themost/common';
import { AsyncSeriesEventEmitter } from '@themost/events';
import { SqlFormatter, QueryExpression } from '@themost/query';

export declare interface DataAdapterTables {
    list(callback: (err: Error, result: { name: string }[]) => void): void;
    listAsync(): Promise<{ name: string }[]>;
}

export declare interface DataAdapterViews {
    list(callback: (err: Error, result: { name: string }[]) => void): void;
    listAsync(): Promise<{ name: string }[]>;
}

export declare class MySqlAdapter implements DataAdapterBase, DataAdapterBaseHelper {
    executing: AsyncSeriesEventEmitter<{target: MySqlAdapter, query: (string | QueryExpression), params?: unknown[]}>;
    executed: AsyncSeriesEventEmitter<{target: MySqlAdapter, query: (string | QueryExpression), params?: unknown[], results: uknown[]}>;

    constructor(options?: any);
    rawConnection?: any;
    options?: any;
    selectIdentityAsync(entity: string, attribute: string): Promise<any>;
    formatType(field: any): string;
    open(callback: (err: Error) => void): void;
    close(callback: (err: Error) => void): void;
    openAsync(): Promise<void>;
    closeAsync(): Promise<void>;
    prepare(query: any, values?: Array<any>): any;
    createView(name: string, query: any, callback: (err: Error) => void): void;
    executeInTransaction(func: any, callback: (err: Error) => void): void;
    executeInTransactionAsync(func: () => Promise<void>): Promise<void>;
    migrate(obj: DataAdapterMigration, callback: (err: Error, result?: any) => void): void;
    migrateAsync(obj: DataAdapterMigration): Promise<any>;
    selectIdentity(entity: string, attribute: string, callback: (err?: Error, value?: any) => void): void;
    execute(query: any, values: any, callback: (err?: Error, values?: any) => void): void;
    executeAsync(query: any, values: any): Promise<any>;
    table(name: string): DataAdapterTable;
    view(name: string): DataAdapterView;
    tables(): DataAdapterTables;
    views(): DataAdapterViews;
    indexes(name: string): DataAdapterIndexes;
    database(name: string): MySqlAdapterDatabase;
    getFormatter(): SqlFormatter;
}