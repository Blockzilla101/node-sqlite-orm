export function getObjDifferences(oldObj: Record<string, any>, newObj: Record<string, any>): Record<string, { old: any; new: any }> {
    const diff: Record<string, { old: any; new: any }> = {};
    for (const [k, v] of Object.entries(oldObj)) {
        // property was removed
        if (!Object.hasOwn(newObj, k)) {
            diff[k] = {
                old: v,
                new: undefined,
            };
        }

        // property was changed
        if (JSON.stringify(oldObj[k]) !== JSON.stringify(newObj[k])) {
            diff[k] = {
                old: v,
                new: newObj[k],
            };
        }
    }

    // new properties
    for (const k of Object.keys(newObj).filter((k) => !Object.keys(oldObj).includes(k))) {
        diff[k] = {
            old: undefined,
            new: newObj[k],
        };
    }

    return diff;
}

export function prettyPrintDiff(oldObj: Record<string, any>, newObj: Record<string, any>): string {
    const diff = getObjDifferences(oldObj, newObj);
    const str: string[] = [];
    for (const [k, v] of Object.entries(diff)) {
        if (v.old === undefined) {
            str.push(`added: ${k}`);
        } else if (v.new === undefined) {
            str.push(`removed: ${k}`);
        } else {
            str.push(`${k}: ${v.old} => ${v.new}`);
        }
    }

    return str.join(', ');
}
