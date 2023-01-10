# node-pg-tasks
Pub/Sub task queue backed by Postgres
# Quickstart
```Javascript
const PgTasks = require('@roonlabs/node-pg-tasks');
const client  = new PgTasks({user: 'postgres', password: 'postgres', database: 'testdb'})

client.subscribe(t => {
    console.log('Received task', t);
    t.ack();
});

(async function() {
    try {
        await client.connect();
        while (true) {
            await new Promise(r => setTimeout(r, 1000));
            await client.publish({ type: 'test', foo: 1 });
        }
    } catch (e) {
        console.log(e);
    }
})()
```
