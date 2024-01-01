import type { Model } from './index.js';
import fs from 'fs';

interface ModelData {
    version: number;
    models: Record<string, Model>;
}

type Versions = Record<string, Model> | ModelData;

function readVersion0(data: Record<string, Model>): ModelData {
    return {
        version: 1,
        models: data,
    };
}

export function read(dbPath: string): Record<string, Model> {
    let data: Versions;

    try {
        data = JSON.parse(fs.readFileSync(`${dbPath}.model.json`).toString('utf-8'));
    } catch (_e) {
        return {};
    }

    if (!('version' in data)) {
        data = readVersion0(data);
    }

    if ('version' in data && data.version === 1) {
        return (data as ModelData).models;
    }

    throw new Error(`unknown version for models ${dbPath}.model.json`);
}

export function write(models: Record<string, Model>, dbPath: string) {
    fs.writeFileSync(
        `${dbPath}.model.json`,
        JSON.stringify(
            {
                version: 1,
                models,
            },
            null,
            2
        )
    );
}
