import { SqlTable } from './index.js';

export class DBError extends Error {}

export class DBNotFound extends DBError {}
export class DBModelNotFound extends DBError {
    constructor(model: typeof SqlTable) {
        super(`${model.name} is not registered`);
    }
}
export class DBInvalidTable extends DBError {}
export class DBInvalidData extends DBError {}
