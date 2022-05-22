// MOST Web Framework Codename Zero Gravity Copyright (c) 2017-2022, THEMOST LP All rights reserved
export declare interface MySqlAdapterTable {
    create(fields: Array<any>, callback: (err: Error) => void): void;
    createAsync(fields: Array<any>): Promise<void>;
    add(fields: Array<any>, callback: (err: Error) => void): void;
    addAsync(fields: Array<any>): Promise<void>;
    change(fields: Array<any>, callback: (err: Error) => void): void;
    changeAsync(fields: Array<any>): Promise<void>;
    exists(callback: (err: Error, result: boolean) => void): void;
    existsAsync(): Promise<boolean>;
    version(callback: (err: Error, result: string) => void): void;
    versionAsync(): Promise<string>;
    columns(callback: (err: Error, result: Array<any>) => void): void;
    columnsAsync(): Promise<Array<any>>;
}

export declare interface MySqlAdapterIndex {
    name: string;
    columns: Array<string>;
}

export declare interface MySqlAdapterIndexes {
    create(name: string, columns: Array<string>, callback: (err: Error, res?: number) => void): void;
    createAsync(name: string, columns: Array<string>): Promise<number>;
    drop(name: string, callback: (err: Error, res?: number) => void): void;
    dropAsync(name: string): Promise<number>;
    list(callback: (err: Error, res: Array<MySqlAdapterIndex>) => void): void;
    listAsync(): Promise<Array<MySqlAdapterIndex>>;
}

export declare interface MySqlAdapterView {
    create(query: any, callback: (err: Error) => void): void;
    createAsync(query: any): Promise<void>;
    exists(callback: (err: Error, result: boolean) => void): void;
    existsAsync(): Promise<boolean>;
    drop(callback: (err: Error) => void): void;
    dropAsync(): Promise<void>;
}

export declare interface MySqlAdapterDatabase {
    exists(callback: (err: Error, result: boolean) => void): void;
    existsAsync(): Promise<boolean>;
    create(callback: (err: Error) => void): void;
    createAsync(): Promise<void>;
}

export declare interface MySqlAdapterMigration {
    add: Array<any>;
    change?: Array<any>;
    appliesTo: string;
    version: string;
    indexes?: Array<{name: string, columns: Array<string>}>;
    updated: boolean;
}

export declare class MySqlAdapter {
    constructor(options?: any);
    formatType(field: any): string;
    open(callback: (err: Error) => void): void;
    close(callback: (err: Error) => void): void;
    openAsync(): Promise<void>;
    closeAsync(): Promise<void>;
    prepare(query: any, values?: Array<any>): any;
    createView(name: string, query: any, callback: (err: Error) => void): void;
    executeInTransaction(func: any, callback: (err: Error) => void): void;
    executeInTransactionAsync(func: Promise<any>): Promise<any>;
    migrate(obj: MySqlAdapterMigration, callback: (err: Error, result?: any) => void): void;
    migrateAsync(obj: MySqlAdapterMigration): Promise<any>;
    selectIdentity(entity: string, attribute: string, callback: (err: Error, value: any) => void): void;
    execute(query: any, values: any, callback: (err: Error, value: any) => void): void;
    executeAsync(query: any, values: any): Promise<any>;
    executeAsync<T>(query: any, values: any): Promise<Array<T>>;
    table(name: string): MySqlAdapterTable;
    view(name: string): MySqlAdapterView;
    indexes(name: string): MySqlAdapterIndexes;
    database(name: string): MySqlAdapterDatabase;
}