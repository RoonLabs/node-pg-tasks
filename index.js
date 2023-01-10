const { Client }   = require('pg');
const EventEmitter = require('node:events');

class PostgresTasks extends EventEmitter {

    #client;
    #config;

    constructor(config = {}) {
        super();
        this.#config = config;
    }

    get connected() {
        return this.#client?._connected;
    }
    
    async connect() {
        this.#client = new Client(this.#config)

        this.#client.on('error', () => {
            console.log('[node-postgres-tasks] Reconnecting...')
            this.#client.end();
            this.connect();
        })
        this.#client.on('connect', () => {
            console.log('[node-postgres-tasks] Connected.');
        })
        this.#client.on('notification', async n => {
            try {
                this.emit('pg-message');
            } catch (e) {
                console.log('[node-postgres-tasks] Error emitting task', e)
            }
        })
        try {
            await this.#client.connect();
            await this.#setup();
        } catch (e) {
            await new Promise(r => setTimeout(r, 1000));
            console.log('[node-postgres-tasks] ', e)
            this.connect();
        }
    }

    async publish(data) {
        const [row] = (await this.#client.query(`INSERT INTO node_pg_tasks (data) VALUES ($1) RETURNING *;`, [data])).rows;
        await this.#client.query(`SELECT pg_notify('node_pg_tasks_channel', '')`);
    }

    async processQueue(callback, seconds) {
        try {
            while (true) {
                const [row] = (await this.#client.query(`
                UPDATE node_pg_tasks SET in_progress = now() + make_interval(secs => $1)
                WHERE id = (
                      SELECT id
                      FROM node_pg_tasks
                      WHERE in_progress is null OR in_progress < now()
                      ORDER BY id
                      FOR UPDATE SKIP LOCKED
                      LIMIT 1
                )
                RETURNING *;`, [seconds])).rows;
                if (!row) return;
                await callback({
                    ...row,
                    ack: async () => {
                        try {
                            await this.#client.query('DELETE FROM node_pg_tasks where id = $1', [row.id]);
                        } catch {}
                    },
                    nack: async () => {
                        try {
                            await this.#client.query('UPDATE node_pg_tasks set in_progress = null where id = $1', [row.id]);
                        } catch {}
                    },
                    extend: async seconds => {
                        try {
                            await this.#client.query(`
                            UPDATE node_pg_tasks set in_progress = in_progress + make_interval(secs => $2) where id = $1`, 
                            [row.id, seconds]);
                        } catch {}
                    }
                });
            }
        } catch (e) {
            console.log('[node-postgres-tasks]', e);
        }
    }

    subscribe(callback, opts = { visibilityTimeOut: 60 }) {
        this.on('pg-message', () => this.processQueue(callback, opts.visibilityTimeOut));
        if (this.connected) {
            this.processQueue(callback, opts.visibilityTimeOut);
        }
        // XXX If pg_notify is unreliable, run processQueue again, loop or timeout or something
    }

    async #setup() {
        try {
            await this.#client.query(`
                CREATE TABLE IF NOT EXISTS node_pg_tasks (
                    id   SERIAL,
                    in_progress timestamp without time zone,
                    data jsonb
                );
            `)
            await this.#client.query('LISTEN "node_pg_tasks_channel"');
            await this.#client.query(`SELECT pg_notify('node_pg_tasks_channel', '')`);
        } catch (e) {
            console.log('[node-postgres-tasks] Critical error during setup, exiting.', e)
            process.exit(1);
        }
    }
}

module.exports = PostgresTasks
