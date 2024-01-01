import SqliteDatabase from 'better-sqlite3';
import {
    buildAggregateQuery,
    buildAlterQuery,
    buildCountWhereQuery,
    buildDeleteQuery,
    buildInsertQuery,
    buildModelFromData,
    buildSelectQuery,
    buildTableQuery,
    buildUpdateQuery,
    isProvidedTypeValid,
} from './builder.js';
import { DBError, DBInvalidData, DBInvalidTable, DBModelNotFound, DBNotFound } from './errors.js';
import { dejsonify, jsonify } from './json.js';
import { prettyPrintDiff } from './util.js';
import { basename, join } from 'path';
import fs from 'fs';
import * as ModelReader from './model-reader.js';
import { spawnSync } from 'child_process';

interface OrmOptions {
    /**
     * Path to database file.
     */
    dbPath: string;
    /**
     * sqlite3 open opts.
     */
    openOptions?: SqliteDatabase.Options;
    /**
     * When set, backups are enabled.
     */
    backupDir?: string;
    /**
     * Delay between each time a backup is attempted.
     * Leave unset if you want to use custom logic (call `orm.doBackup('auto')`)
     */
    backupInterval?: number;
    /**
     * When set, each backup is placed under `<backup-dir>/current-git-branch/`.
     * Backups are also written with current git commit.
     */
    backupUseGitCommit?: boolean;
    /**
     * Maximum number of auto backups (default 10)
     */
    backupMax?: number;
    /**
     * Set to true if you are using an existing database that contains JSON.
     * The library uses a custom parser to write maps and other class instances
     * so that it can read them as the class instance. (Custom classes should be
     * registered with `@registerJsonSerializable()`)
     */
    jsonCompatMode?: boolean;
}

export type ColumnType = 'string' | 'number' | 'boolean' | 'json' | 'integer' | 'blob';

export class SqlTable {
    public _new = true;
    public id = -1;
}

export interface TableColumn {
    /**
     * Type of column
     */
    type: ColumnType;
    /**
     * Name of column, preferably same as one in the sqlite database.
     */
    name: string;
    /**
     * Incase the column has a different name in the sqlite database.
     */
    mappedTo?: string;
    /**
     * Whether this column can be set to null.
     */
    nullable: boolean;
    /**
     * The default value of this column.
     */
    defaultValue: any;
    /**
     * Whether this column is a primary key.
     */
    isPrimaryKey: boolean;
    /**
     * Whether this column is automatically incremented.
     * Only valid for type 'integer' columns.
     */
    autoIncrement: boolean;
}

export interface WhereClause {
    where: {
        clause: string;
        values?: any[];
    };
}

export interface OrderClause {
    order: {
        by: string;
        desc?: boolean;
    };
}

export interface AggregateClause {
    select: {
        clause: string;
    };
}

export interface GroupByClause {
    group: {
        cols: string[];
    };
}

export interface HavingClause {
    having: {
        clause: string;
        values?: any[];
    };
}

export interface SelectQuery extends Partial<WhereClause>, Partial<OrderClause> {
    limit?: number;
    offset?: number;
}

export interface AggregateSelectQuery
    extends SelectQuery,
        AggregateClause,
        GroupByClause,
        Partial<HavingClause> {}

export type PrimitiveTypes = number | string | boolean;

// delete doesn't require a where clause
export type DeleteQuery = Partial<SelectQuery>;

export class Model {
    constructor(
        public tableName: string,
        public columns: TableColumn[],
        public readonly database: string
    ) {}
}

const gitBranch = spawnSync('git', ['branch', '--show-current'], {
    stdio: 'pipe',
})
    .stdout.toString('utf-8')
    .trim();

const gitCommit = spawnSync('git', ['rev-parse', 'HEAD'], {
    stdio: 'pipe',
})
    .stdout.toString('utf-8')
    .trim();

export class SqliteOrm {
    public db: SqliteDatabase.Database;
    private hasChangesSinceBackup = false;
    private backupsEnabled = false;
    private hasModelChanges = false;
    private attachedDatabases: string[] = [];

    public models: Record<string, Model> = {};

    private tempModelData: TableColumn[] = [];
    private ignoredColumns: string[] = [];

    private opts: OrmOptions;
    private lastModels: Record<string, Model> = {};

    private constructor(options: OrmOptions) {
        this.opts = options;
        if (this.opts.backupDir) {
            this.backupsEnabled = true;
            try {
                fs.statSync(this.opts.backupDir);
            } catch (_e) {
                fs.mkdirSync(this.opts.backupDir);
            }
        }

        if (this.opts.backupUseGitCommit && this.backupsEnabled) {
            try {
                fs.statSync(join(this.opts.backupDir!, gitBranch));
            } catch (_e) {
                fs.mkdirSync(join(this.opts.backupDir!, gitBranch));
            }
        }

        if (this.opts.backupInterval && this.backupsEnabled) {
            setInterval(() => {
                this.doBackup('auto');
            }, this.opts.backupInterval);
        }

        SqliteOrm.logInfo(this.opts, 'opening database');

        this.db = new SqliteDatabase(options.dbPath, options.openOptions);

        this.lastModels = ModelReader.read(options.dbPath);
    }

    //#region table logic

    public findOne<T extends SqlTable>(table: new () => T, idOrQuery: PrimitiveTypes | SelectQuery): T {
        if (this.models[table.name] == null) throw new DBModelNotFound(table);

        const col = this.models[table.name].columns.find((c) => c.isPrimaryKey);
        if (col == null)
            throw new DBInvalidTable(`${this.models[table.name].tableName} does not have primary key`);
        if (typeof idOrQuery !== 'object' && !isProvidedTypeValid(idOrQuery, col))
            throw new DBInvalidData(`${this.models[table.name].tableName}.${col.name} has a different type`);

        const query = buildSelectQuery(
            typeof idOrQuery === 'object'
                ? { ...idOrQuery, limit: 1 }
                : {
                      where: {
                          clause: `${col.mappedTo ?? col.name} = ?`,
                          values: [this.serialize(idOrQuery, col.type)],
                      },
                      limit: 1,
                  },
            this.models[table.name]
        );

        const found = this.db.prepare(query.query).get(...query.params) as Record<string, unknown>;
        if (!found) {
            if (typeof idOrQuery === 'object') {
                throw new DBNotFound(`query did not match any items in ${table.name}`);
            } else {
                throw new DBNotFound(
                    `row with ${col.name} = ${idOrQuery} was not found in table ${table.name}`
                );
            }
        }

        const parsed = new table();
        for (const col of this.models[table.name].columns) {
            (parsed as Record<string, unknown>)[col.name] = this.deserialize(
                found[col.mappedTo ?? col.name],
                col.type
            );
        }
        parsed._new = false;

        return parsed;
    }

    public findOneOptional<T extends SqlTable>(
        table: new () => T,
        idOrQuery: PrimitiveTypes | SelectQuery
    ): T {
        try {
            return this.findOne(table, idOrQuery);
        } catch (e) {
            if (e instanceof DBNotFound) {
                return new table();
            }
            throw e;
        }
    }

    public findMany<T extends SqlTable>(table: new () => T, query: SelectQuery): T[] {
        if (this.models[table.name] == null) throw new DBModelNotFound(table);

        const builtQuery = buildSelectQuery(query, this.models[table.name]);

        const data = this.db.prepare(builtQuery.query).all(...builtQuery.params) as Record<string, unknown>[];
        const parsedAll: T[] = [];

        for (const datum of data) {
            const parsed = new table();
            for (const col of this.models[table.name].columns) {
                (parsed as Record<string, unknown>)[col.name] = this.deserialize(
                    datum[col.mappedTo ?? col.name],
                    col.type
                );
            }
            parsed._new = false;
            parsedAll.push(parsed);
        }

        return parsedAll;
    }

    public countWhere<T extends SqlTable>(table: new () => T, query: WhereClause): number {
        if (this.models[table.name] == null) throw new DBModelNotFound(table);

        const builtQuery = buildCountWhereQuery(query, this.models[table.name]);
        return (this.db.prepare(builtQuery.query).get(...builtQuery.params) as { 'COUNT(*)': number })[
            'COUNT(*)'
        ];
    }

    public aggregateSelect<Row extends Array<any>, T extends SqlTable = SqlTable>(
        table: new () => T,
        query: AggregateSelectQuery
    ): Row[] {
        if (this.models[table.name] == null) throw new DBModelNotFound(table);

        const builtQuery = buildAggregateQuery(query, this.models[table.name]);
        return this.db.prepare(builtQuery.query).all(...builtQuery.params) as Row[];
    }

    public save<T extends SqlTable>(obj: T): T {
        const model = this.models[obj.constructor.name];
        if (model == null) throw new DBModelNotFound(obj.constructor as typeof SqlTable);

        const builtData: Record<string, unknown> = {};
        model.columns.forEach((col) => {
            builtData[col.mappedTo ?? col.name] = this.serialize(
                (obj as Record<string, unknown>)[col.name],
                col.type
            );
        });

        if (obj._new) {
            const builtQuery = buildInsertQuery(model, builtData);
            const result = this.db.prepare(builtQuery.query).run(...builtQuery.params);

            const incrementPrimaryKey = model.columns.find((c) => c.isPrimaryKey && c.autoIncrement);
            if (incrementPrimaryKey) {
                (obj as Record<string, unknown>)[incrementPrimaryKey.name] = result.lastInsertRowid;
            }

            obj._new = false;
        } else {
            const builtQuery = buildUpdateQuery(model, builtData);
            this.db.prepare(builtQuery.query).run(...builtQuery.params);
        }
        this.hasChangesSinceBackup = true;

        return obj;
    }

    public delete<T extends SqlTable>(table: new () => T, query: DeleteQuery) {
        const built = buildDeleteQuery(query, this.models[table.name]);
        this.db.prepare(built.query).run(...built.params);
        this.hasChangesSinceBackup = true;
    }

    //#endregion table logic

    //#region decorators

    /**
     * Explicity set column type for a model otherwise its inferred from default value.
     * @param type type of table column
     * @param nullable whether column can have a null value, defaults to true when property value is `undefined` or `null`
     */
    public column(data: Partial<TableColumn>) {
        return (model: { constructor: new () => SqlTable } | SqlTable, propertyKey: string) => {
            if (data.isPrimaryKey && this.tempModelData.find((i) => i.isPrimaryKey))
                throw new DBInvalidTable(
                    `${
                        model.constructor.name
                    }: table cannot have two primary keys, existing key (${this.tempModelData.find(
                        (i) => i.isPrimaryKey
                    )})`
                );
            this.createTempColumn(
                data,
                new (model as { constructor: new () => SqlTable }).constructor(),
                propertyKey
            );
        };
    }

    /**
     * Sets type of data the column has.
     * @param type column data type
     */
    public columnType(type: ColumnType) {
        return this.column({ type });
    }

    /**
     * Marks a column as nullable.
     */
    public nullable(nullable = true) {
        return this.column({ nullable });
    }

    /**
     * Marks a column as primary key
     */
    public primaryKey(primaryKey = true) {
        return this.column({
            isPrimaryKey: primaryKey,
        });
    }

    /**
     * Maps property to an existing column.
     * @param oldColumnName name of existing column
     */
    public mapTo(mappedTo: string) {
        return this.column({
            mappedTo,
        });
    }

    /**
     * Maps property to an existing column.
     * @param oldColumnName name of existing column
     */
    public autoIncrement(autoIncrement: boolean) {
        return this.column({
            autoIncrement,
        });
    }

    /**
     * Property is not considered a column.
     */
    public ignoreColumn() {
        return (_model: SqlTable, propertyKey: string) => {
            const index = this.tempModelData.findIndex((i) => i.name === propertyKey);
            if (index > -1) {
                this.tempModelData.splice(index, 1);
            }
            this.ignoredColumns.push(propertyKey);
        };
    }

    // todo use an object
    /**
     * Adds a class to orm models.
     * @param tableName name of table in database
     */
    public model(tableName?: string, database = 'main') {
        return (model: new () => SqlTable) => {
            const tempModel = new model();
            const hasPrimaryKey = this.tempModelData.find((i) => i.isPrimaryKey) != null;

            for (const [k, v] of Object.entries(tempModel)) {
                if (this.ignoredColumns.includes(k)) continue;
                if (v == null && this.tempModelData.find((i) => i.name === k) == null)
                    throw new DBInvalidTable(
                        `${tableName ?? model.name}.${k}: Cannot infer type from a null value property`
                    );

                // ignore types other then string, number, boolean or object
                if (
                    typeof v !== 'string' &&
                    typeof v !== 'boolean' &&
                    typeof v !== 'object' &&
                    typeof v !== 'number'
                )
                    continue;

                if (hasPrimaryKey && k === 'id') continue;
                if (k.startsWith('_')) continue;

                let type: ColumnType;
                if (this.tempModelData.find((i) => i.name === k) == null) {
                    if (typeof v === 'object') {
                        type = 'json';
                    } else if (typeof v === 'number') {
                        type = 'integer';
                    } else {
                        type = typeof v as ColumnType;
                    }
                } else {
                    type = this.tempModelData.find((i) => i.name === k)!.type;
                }

                this.createTempColumn(
                    {
                        defaultValue: v,
                        nullable: v == null,
                        name: k,
                        type: type,
                        isPrimaryKey: !hasPrimaryKey && k === 'id',
                        autoIncrement: k === 'id' && !hasPrimaryKey,
                    },
                    tempModel,
                    k
                );
            }

            const builtModel = new Model(tableName ?? model.name, this.tempModelData, database);
            this.models[model.name] = builtModel;
            this.tempModelData = [];

            // create table if it doesn't exist
            const info = this.db.prepare(`PRAGMA ${database}.table_info('${model.name}')`).all() as any[];
            if (info.length === 0) {
                this.hasModelChanges = true;
                this.db.exec(buildTableQuery(builtModel));
            } else {
                // add missing columns
                // const info = this.db.prepare(`PRAGMA ${database}.table_info('${model.name}')`);
                buildAlterQuery(buildModelFromData(builtModel, info), builtModel).forEach((c: string) => {
                    this.hasModelChanges = true;
                    this.db.exec(c);
                });
            }

            if (this.lastModels[model.name] == null) {
                // new a model was added
                this.hasModelChanges = true;
                SqliteOrm.logInfo(this.opts, `found new table ${model.name}`);
            } else {
                const oldCols = this.lastModels[model.name].columns;
                const newCols = builtModel.columns;
                const oldDatabase = this.lastModels[model.name].database;

                if (oldDatabase != null && oldDatabase !== database) {
                    SqliteOrm.logInfo(this.opts, `database change from ${oldDatabase} to ${database}`);
                    this.hasModelChanges = true;
                }

                for (const oldCol of oldCols) {
                    const newCol = newCols.find(
                        (c) => (c.mappedTo ?? c.name) === (oldCol.mappedTo ?? oldCol.name)
                    );
                    // missing col
                    if (newCol == null) {
                        SqliteOrm.logInfo(this.opts, `[${model.name}] column ${oldCol.name} was removed`);
                        this.hasModelChanges = true;
                        continue;
                    }

                    // changed col
                    const diff = prettyPrintDiff(
                        { ...oldCol, defaultValue: undefined },
                        { ...newCol, defaultValue: undefined }
                    );
                    if (diff.length > 0) {
                        this.hasModelChanges = true;
                        SqliteOrm.logInfo(
                            this.opts,
                            `[${model.name}] column ${newCol.name} was changed: ${diff}`
                        );
                    }
                }

                // new col
                for (const newCol of newCols.filter(
                    (c) => oldCols.find((o) => (o.mappedTo ?? o.name) === (c.mappedTo ?? c.name)) == null
                )) {
                    SqliteOrm.logInfo(this.opts, `[${model.name}] column ${newCol.name} was added`);
                    this.hasModelChanges = true;
                }
            }
        };
    }

    //#endregion decorators

    //#region logging

    public static logInfo: (dbOptions: OrmOptions, ...msg: any[]) => void = () => {};

    public static logDebug: (dbOptions: OrmOptions, ...msg: any[]) => void = () => {};

    //#endregion logging

    //#region backup

    public manualBackup() {
        this.doBackup('manual');
    }

    public doBackup(type: 'auto' | 'model-changes' | 'manual') {
        if (!this.backupsEnabled) return;
        if (!this.hasChangesSinceBackup && type === 'auto') return;
        const filePath = join(this.backupDir(), this.backupName(type));
        fs.copyFileSync(this.opts.dbPath, filePath);

        let backupCount = 0;
        let oldestBackup = '';
        let oldestTime = new Date();
        for (const backup of fs.readdirSync(this.backupDir())) {
            if (backup.startsWith('auto-')) {
                const [, createDate] = backup.split('-');
                const date = new Date(createDate);
                if (date < oldestTime) {
                    oldestTime = date;
                    oldestBackup = backup;
                }

                backupCount++;
            }
        }

        if (backupCount > (this.opts.backupMax ?? 10)) {
            fs.unlinkSync(join(this.backupDir(), oldestBackup));
        }

        SqliteOrm.logInfo(
            this.opts,
            `Created a backup to <backup-dir>${
                this.opts.backupUseGitCommit ? `/${gitBranch}` : ''
            }/${this.backupName(type)}`
        );

        this.hasChangesSinceBackup = false;
    }

    private backupDir() {
        if (!this.opts.backupUseGitCommit) return this.opts.backupDir!;
        return join(this.opts.backupDir!, gitBranch);
    }

    private backupName(type: 'auto' | 'model-changes' | 'manual') {
        const dbName = basename(this.opts.dbPath);
        if (this.opts.backupUseGitCommit) {
            return `${type}-${new Date().toISOString()}-${gitCommit}-${dbName}`;
        }
        return `${type}-${new Date().toISOString()}-${dbName}`;
    }

    //#endregion backup

    //#region misc

    /**
     * Cleanly close the database.
     */
    public close() {
        this.db.close();
    }

    /**
     * Should be called when all models are loaded. If backups are enabled a
     * backup is created if tables were been modified.
     */
    public modelsLoaded() {
        for (const m of Object.keys(this.lastModels).filter((k) => this.models[k] == null)) {
            SqliteOrm.logInfo(this.opts, `${m} was removed`);
            this.hasModelChanges = true;
        }

        if (this.hasModelChanges) {
            this.doBackup('model-changes');
        }

        this.hasModelChanges = false;
        this.saveModel();
    }

    public attach(databasePath: string, name?: string) {
        if (name == null) {
            name = basename(databasePath);
        }

        if (this.attachedDatabases.includes(name)) throw new DBError(`${databasePath} is already attached`);
        this.db.prepare('ATTACH DATABASE ? AS ?').run(databasePath, name);
        this.attachedDatabases.push(name);

        SqliteOrm.logInfo(this.opts, `attached ${databasePath} as ${name}`);
    }

    //#endregion misc

    private createTempColumn(
        data: Partial<TableColumn>,
        model: SqlTable & Record<string, any>,
        propertyKey: string
    ) {
        const index = this.tempModelData.findIndex((i) => i.name == propertyKey);
        if (index > -1) {
            Object.assign(this.tempModelData[index], data);
            return;
        }

        if (
            typeof model[propertyKey] !== 'string' &&
            typeof model[propertyKey] !== 'boolean' &&
            typeof model[propertyKey] !== 'number' &&
            typeof model[propertyKey] !== 'object' &&
            model[propertyKey] != null
        )
            throw new DBInvalidTable(
                `${model.constructor.name}.${propertyKey} has an invalid type, (${typeof model[
                    propertyKey
                ]} is not valid)`
            );

        data.name = propertyKey;
        data.defaultValue = model[propertyKey];

        if (data.type == null) {
            if (model[propertyKey] == null)
                throw new DBInvalidTable(
                    `${model.constructor.name}.${propertyKey}: type must be specified for a column with null value`
                );
            data.type =
                typeof model[propertyKey] == 'object' ? 'json' : (typeof model[propertyKey] as ColumnType);
        }

        if (
            data.isPrimaryKey == null &&
            !this.tempModelData.find((i) => i.isPrimaryKey) &&
            data.name === 'id'
        ) {
            data.isPrimaryKey = true;
        } else if (data.isPrimaryKey == null) {
            data.isPrimaryKey = false;
        }

        if (data.nullable == null && model[propertyKey] == null) {
            data.nullable = true;
        }

        if (data.autoIncrement == null) {
            data.autoIncrement = false;
        }

        this.tempModelData.push(data as Required<TableColumn>);
    }

    private serialize(data: any, type: ColumnType) {
        if (data == null) return null;
        switch (type) {
            case 'boolean': {
                if (typeof data !== 'boolean')
                    throw new DBInvalidData('Cannot store a non boolean type on a boolean column');
                return data ? 1 : 0;
            }
            case 'string': {
                if (typeof data !== 'string')
                    throw new DBInvalidData('Cannot store a non string type on a string column');
                return data;
            }
            case 'number': {
                if (typeof data !== 'number')
                    throw new DBInvalidData('Cannot store a non number type on a number column');
                return data;
            }
            case 'integer': {
                if (typeof data !== 'number' || !Number.isInteger(data))
                    throw new DBInvalidData('Cannot store a non integer type on an integer column');
                return data;
            }
            case 'blob': {
                if (!(data instanceof Uint8Array))
                    throw new DBInvalidData('Cannot store a blob/u8int[] type on a blob column');
                return data;
            }
            case 'json': {
                if (typeof data !== 'object')
                    throw new DBInvalidData('Cannot convert non object type into JSON');
                return JSON.stringify(jsonify(data));
            }
            default: {
                throw new Error(`Unknown column type: ${type}`);
            }
        }
    }

    private deserialize(data: any, type: ColumnType) {
        if (data == null) return null;
        switch (type) {
            case 'boolean': {
                if (typeof data !== 'number' && !Number.isInteger(data))
                    throw new DBInvalidData(`Column contains ${data} instead of a boolean`);
                return data === 1 ? true : false;
            }
            case 'string': {
                if (typeof data !== 'string')
                    throw new DBInvalidData(`Column contains ${data} instead of a string`);
                return data;
            }
            case 'number': {
                if (typeof data !== 'number')
                    throw new DBInvalidData(`Column contains ${data} instead of a number`);
                return data;
            }
            case 'integer': {
                if (typeof data !== 'number' && !Number.isInteger(data))
                    throw new DBInvalidData(`Column contains ${data} instead of an integer`);
                return data;
            }
            case 'blob': {
                if (!(data instanceof Uint8Array))
                    throw new DBInvalidData(`Column contains ${typeof data} instead of a blob`);
                return data;
            }
            case 'json': {
                if (typeof data !== 'string')
                    throw new DBInvalidData(`Column contains ${typeof data} instead of a JSON string`);
                try {
                    const parsed = JSON.parse(data);
                    return dejsonify(parsed, this.opts.jsonCompatMode ?? false);
                } catch (e) {
                    throw new DBInvalidData('Column contains invalid JSON data', { cause: e });
                }
            }
        }
    }

    private saveModel() {
        ModelReader.write(this.models, this.opts.dbPath);
    }
}
