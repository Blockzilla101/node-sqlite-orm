import { jsonify } from './json.js';
import {
    AggregateSelectQuery,
    ColumnType,
    DeleteQuery,
    Model,
    SelectQuery,
    TableColumn,
    WhereClause,
} from './index.js';

interface BuiltQuery {
    query: string;
    params: any[];
}

function getSqlType(type: ColumnType) {
    switch (type) {
        case 'boolean':
        case 'integer':
            return 'INTEGER';
        case 'json':
        case 'string':
            return 'TEXT';
        case 'blob':
            return 'BLOB';
        case 'number':
            return 'REAL';
        default:
            throw new Error('invalid column type');
    }
}

function getDefaultValue(type: ColumnType, value: any) {
    switch (type) {
        case 'boolean':
            return value ? 1 : 0;
        case 'number':
        case 'integer':
            return value;
        case 'json':
            return `'${JSON.stringify(jsonify(value))}'`;
        case 'string':
            return `'${value}'`;
        case 'blob':
            return value;
        default:
            throw new Error('invalid column type');
    }
}

export function buildTableQuery(model: Model) {
    const str = [`CREATE TABLE ${model.database}.'${model.tableName}' (`];
    for (const column of model.columns) {
        str.push(buildColumnQuery(column) + ',');
    }
    str[str.length - 1] = str[str.length - 1].slice(0, -1);
    str.push(')');

    return str.join('\n');
}

export function buildColumnQuery(column: TableColumn) {
    if (column.autoIncrement && column.type != 'integer')
        throw new Error('Auto increment cannot be used on non integer column.');
    return `"${column.name}" ${getSqlType(column.type)} ${column.nullable ? '' : 'NOT NULL'} ${
        column.defaultValue == null && !column.autoIncrement
            ? ''
            : 'DEFAULT ' + getDefaultValue(column.type, column.defaultValue)
    } ${column.isPrimaryKey ? 'PRIMARY KEY' : ''} ${column.autoIncrement ? 'AUTOINCREMENT' : ''}`;
}

export function buildAlterQuery(existingModel: Model, actualModel: Model) {
    // shouldn't be possible
    if (existingModel.tableName !== actualModel.tableName)
        throw new Error(
            `[internal] table names are different (${existingModel.tableName} != ${actualModel.tableName})`
        );
    if (
        existingModel.columns.find((c) => c.isPrimaryKey)?.name !==
        actualModel.columns.find((c) => c.isPrimaryKey)?.name
    )
        throw new Error(
            `${existingModel.tableName}: Cannot add a column automatically, a primary key column already exists.`
        );

    return actualModel.columns
        .filter(
            (col) => existingModel.columns.find((c) => c.name === col.name || c.name === col.mappedTo) == null
        )
        .map(
            (col) =>
                `ALTER TABLE ${actualModel.database}.'${actualModel.tableName}' ADD COLUMN ${buildColumnQuery(
                    col
                )}`
        );
}

export function buildModelFromData(ogModel: Model, data: any[]): Model {
    const cols: TableColumn[] = [];

    for (const datum of data) {
        const ogCol = ogModel.columns.find((t) => t.name === datum.name || t.mappedTo === datum.name);
        if (ogCol == null) continue;

        cols.push({
            defaultValue: datum.dflt_value,
            name: datum.name,
            nullable: datum.notnull == 0,
            type: ogCol.type,
            isPrimaryKey: datum.pk == 1,
            mappedTo: ogCol.mappedTo,
            autoIncrement: ogCol.autoIncrement,
        });
    }

    return new Model(ogModel.tableName, cols, ogModel.database);
}

function buildBaseFilterQuery(query: Partial<SelectQuery>): BuiltQuery {
    const str: string[] = [];

    if (query.where) {
        str.push(`WHERE ${query.where.clause}`);
    }

    if (query.order) {
        str.push(`ORDER BY ${query.order.by}${query.order.desc ? ' DESC' : ''}`);
    }

    if (query.limit) {
        str.push(`LIMIT ${query.limit}`);
    }

    if (query.offset) {
        str.push(`OFFSET ${query.offset}`);
    }

    return {
        query: str.join(' '),
        params: query.where?.values ?? [],
    };
}

export function buildSelectQuery(query: SelectQuery, model: Model): BuiltQuery {
    const base = buildBaseFilterQuery(query);
    base.query = `SELECT * FROM ${model.database}.'${model.tableName}' ${base.query}`;
    return base;
}

export function buildDeleteQuery(query: DeleteQuery, model: Model): BuiltQuery {
    const base = buildBaseFilterQuery(query);
    base.query = `DELETE FROM ${model.database}.'${model.tableName}' ${base.query}`;
    return base;
}

export function buildInsertQuery(model: Model, data: Record<string, unknown>): BuiltQuery {
    const params: any[] = [];
    const cols: string[] = [];
    for (const [col, value] of Object.entries(data)) {
        const modelCol = model.columns.find(
            (c) => c.name === col || c.mappedTo === col
        ) as NonNullable<TableColumn>;
        if (modelCol.isPrimaryKey && modelCol.autoIncrement) continue;
        cols.push(col);
        params.push(value);
    }

    return {
        query: `INSERT INTO '${model.tableName}' (${cols.join(', ')}) VALUES (${cols
            .map(() => '?')
            .join(', ')})`,
        params,
    };
}

export function buildUpdateQuery(model: Model, data: Record<string, unknown>): BuiltQuery {
    const params: any[] = [];
    const cols: string[] = [];

    let primaryCol;
    let primaryVal;

    for (const [col, value] of Object.entries(data)) {
        const modelCol = model.columns.find(
            (c) => c.name === col || c.mappedTo === col
        ) as NonNullable<TableColumn>;

        if (modelCol.isPrimaryKey) {
            primaryCol = modelCol.mappedTo ?? modelCol.name;
            primaryVal = value;
            continue;
        }

        cols.push(col);
        params.push(value);
    }

    return {
        query: `UPDATE '${model.tableName}' SET ${cols
            .map((c) => `${c} = ?`)
            .join(', ')} WHERE ${primaryCol} = ?`,
        params: [...params, primaryVal],
    };
}

export function buildCountWhereQuery(query: WhereClause, model: Model): BuiltQuery {
    return {
        query: `SELECT COUNT(*) FROM ${model.database}.'${model.tableName}' WHERE ${query.where.clause}`,
        params: query.where.values ?? [],
    };
}

export function buildAggregateQuery(query: AggregateSelectQuery, model: Model): BuiltQuery {
    const params: any[] = [];
    const str = [`SELECT ${query.select.clause} FROM ${model.database}.'${model.tableName}'`];

    if (query.where) {
        str.push(`WHERE ${query.where.clause}`);
        params.push(...(query.where.values ?? []));
    }

    if (query.group) {
        str.push(`GROUP BY ${query.group.cols.join(', ')}`);
    }

    if (query.having) {
        str.push(`HAVING ${query.having.clause}`);
        params.push(...(query.having.values ?? []));
    }

    if (query.order) {
        str.push(`ORDER BY ${query.order.by}${query.order.desc ? ' DESC' : ''}`);
    }

    if (query.limit) {
        str.push(`LIMIT ${query.limit}`);
    }

    if (query.offset) {
        str.push(`OFFSET ${query.offset}`);
    }

    return {
        query: str.join(' '),
        params: params,
    };
}

export function isProvidedTypeValid(provType: any, col: TableColumn): boolean {
    if (col.nullable && provType == null) return true;
    if (typeof provType === 'number' && !Number.isFinite(provType)) return false;
    switch (col.type) {
        case 'string':
            return typeof provType === 'string';
        case 'boolean':
            return typeof provType === 'boolean' || provType === 0 || provType === 1;
        case 'integer':
            return typeof provType === 'number' && Number.isSafeInteger(provType);
        case 'number':
            return typeof provType === 'number';
        case 'json':
            return typeof provType === 'object' || provType instanceof Array;
        case 'blob':
            return provType instanceof Uint8Array;
    }
}
