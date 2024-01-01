const serializableClasses: {
    classRef: new () => any;
    ignoredProps: string[];
}[] = [];

export function registerJsonSerializable(ignoredProps: string[] = []) {
    return (clas: new () => any) => {
        serializableClasses.push({
            classRef: clas,
            ignoredProps: ignoredProps,
        });
    };
}

function isSerializable(obj: any): boolean {
    return typeof obj !== 'bigint' && typeof obj !== 'function' && typeof obj !== 'symbol';
}

function writeValue(val: any) {
    if (val == null) return null;
    switch (typeof val) {
        case 'boolean':
            return val;
        case 'number':
            return val;
        case 'string':
            return val;
        case 'object': {
            if (val instanceof Map) {
                return {
                    data: jsonify([...val.entries()]),
                    type: 'Map',
                };
            }

            if (val instanceof Array) {
                return jsonify(val);
            }

            if (val instanceof Buffer) {
                return {
                    data: val.toString('base64'),
                    type: 'Buffer',
                };
            }

            const clas = serializableClasses.find((c) => val instanceof c.classRef);
            if (clas != null) {
                return {
                    data: jsonify(val, clas.ignoredProps),
                    type: `custom-${clas.classRef.name}`,
                };
            }

            return {
                data: jsonify(val),
                type: 'object',
            };
        }
        default:
            throw new Error(`Cannot write object of type ${typeof val}`);
    }
}

function readValue(val: any, compatMode: boolean) {
    if (val == null) return null;
    switch (typeof val) {
        case 'boolean':
            return val;
        case 'number':
            return val;
        case 'string':
            return val;
        case 'object': {
            if (val instanceof Array) {
                return dejsonify(val, compatMode);
            }

            if (val.type == null) {
                if (compatMode) return dejsonify(val, compatMode);
                throw new Error('JSON object has a null type');
            }

            if (val.type === 'Map') {
                return new Map(dejsonify(val.data, compatMode));
            }

            if (val.type === 'Buffer') {
                return Buffer.from(val.data, 'base64');
            }

            if (val.type === 'object') {
                return dejsonify(val.data, compatMode);
            }

            if (val.type.startsWith('custom-')) {
                const clas = serializableClasses.find(
                    (c) => c.classRef.name === val.type.slice('custom-'.length)
                );
                if (clas != null) {
                    const obj = new clas.classRef();
                    Object.assign(obj, dejsonify(val.data, compatMode));
                    return obj;
                }
                throw new Error(`Class for type '${val.type.slice('custom-'.length)}' was not registered`);
            }

            if (compatMode) return dejsonify(val, compatMode);
            throw new Error(`JSON object is of unknown type ${val.type}`);
        }
        default:
            throw new Error(`Cannot read object of type ${typeof val}`);
    }
}

// not really indented for directly serializing maps, custom classes etc
/**
 * @param obj Object to convert to JSON
 * @returns JSON safe object
 */
export function jsonify(obj: Record<string, any> | any[], ignoredProps: string[] = []): any {
    // parse arrays
    if (obj instanceof Array) {
        const parsed: any[] = [];
        for (const item of obj) {
            if (!isSerializable(item)) continue;
            parsed.push(writeValue(item));
        }
        return parsed;
    }

    // parse objects
    const parsed: Record<string, any> = {};
    for (const [key, value] of Object.entries(obj)) {
        if (!isSerializable(value)) continue;
        if (ignoredProps.includes(key)) continue;
        parsed[key] = writeValue(value);
    }

    return parsed;
}

/**
 * @param json JSON safe object
 * @returns object before serializing
 */
export function dejsonify(jsonObj: Record<string, any> | any[], compatMode: boolean): any {
    if (jsonObj instanceof Array) {
        const parsed: any[] = [];
        for (const item of jsonObj) {
            parsed.push(readValue(item, compatMode));
        }
        return parsed;
    }

    // parse objects
    const parsed: Record<string, any> = {};
    for (const [key, value] of Object.entries(jsonObj)) {
        parsed[key] = readValue(value, compatMode);
    }

    return parsed;
}
