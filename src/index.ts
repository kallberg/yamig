import { program as originalProgram } from "commander";
import { promises } from "fs";
import { createPool, DatabasePoolType, sql } from "slonik";
import { Pool, PoolClient } from "pg";
import pino from "pino";

const logger = pino({
  prettyPrint: { colorize: true },
  base: undefined,
});

const program = originalProgram.hook("preAction", (cmd, action) => {
  const opts = cmd.opts();

  const options: { [key: string]: unknown } = { ...defaultOptions, ...opts };

  for (const key of Object.keys(options)) {
    action.setOptionValue(key, options[key]);
  }
});

const fs = promises;

type Migration = {
  id: number;
  name: string;
  sql: string;
  up: boolean;
};

type MigrationRecord = {
  id: number;
  name: string;
  applied: Date;
};

type Options = {
  host: string;
  port: number;
  username: string;
  password: string | undefined;
  dbname: string | undefined;
};

const defaultOptions = {
  host: "localhost",
  port: 5432,
  username: "postgres",
  password: undefined,
  database: undefined,
};

function notNull<T>(value: T | null): value is T {
  if (value === null) return false;
  return true;
}

const poolFromOptions = ({
  username,
  password,
  host,
  port,
  dbname,
}: Options): DatabasePoolType => {
  return createPool(
    `postgres://${
      password ? `${username}:${password}` : username
    }@${host}:${port}/${dbname}`
  );
};

const pgPoolFromOptions = ({
  username,
  password,
  host,
  port,
  dbname,
}: Options): Pool => {
  return new Pool({
    user: username,
    host,
    password,
    port,
    database: dbname,
  });
};

const ensureMigrationDirectory = async () => {
  const stat = await fs.stat("migrations").catch(async () => {
    await fs.mkdir("migrations");
    return await fs.stat("migrations");
  });

  if (!stat.isDirectory()) {
    throw new Error("Expected 'migrations' to be a directory");
  }
};

const ensureMigrationTable = async (
  pool: DatabasePoolType
): Promise<unknown> => {
  return pool.query(sql`
    CREATE TABLE IF NOT EXISTS migration (
      id bigint NOT NULL CONSTRAINT migration_pk PRIMARY KEY,
      name text NOT NULL,
      applied timestamp with time zone DEFAULT NOW() NOT NULL
    );
  `);
};

const fetchMigrationRecords = async (
  pool: DatabasePoolType
): Promise<readonly MigrationRecord[]> => {
  await ensureMigrationTable(pool);

  return pool.any<MigrationRecord>(sql`SELECT * FROM migration`);
};

const readLocalMigration = async (
  id: number,
  name: string,
  up: boolean
): Promise<Migration> => {
  const sqlSrc = await fs
    .readFile(`migrations/${id}-${name}-${up ? "up.sql" : "down.sql"}`)
    .then((buffer) => buffer.toString("utf-8"));

  return {
    id,
    name,
    sql: sqlSrc,
    up,
  };
};

const findLocalMigrations = async (up: boolean): Promise<Migration[]> => {
  await ensureMigrationDirectory();
  const matches: RegExpMatchArray[] = await fs
    .readdir("migrations")
    .then((contents) =>
      contents.map((item) => {
        if (up) {
          return item.match(/^(?<id>\d{13})-(?<name>.*)-(up\.sql)$/);
        } else {
          return item.match(/^(?<id>\d{13})-(?<name>.*)-(down\.sql)$/);
        }
      })
    )
    .then((results) => results.filter<RegExpMatchArray>(notNull));

  const migrations: Migration[] = [];

  for (const { groups } of matches) {
    if (!groups) {
      throw new Error("Parse error");
    }

    const { id, name } = groups;

    const migration = await readLocalMigration(parseInt(id), name, up);

    migrations.push(migration);
  }

  return migrations;
};

const fetchMigrationHeadRecord = async (
  pool: DatabasePoolType
): Promise<MigrationRecord | null> => {
  await ensureMigrationTable(pool);

  return pool.maybeOne<MigrationRecord>(
    sql`SELECT * FROM migration ORDER BY id DESC LIMIT 1`
  );
};

const cmdNew = async (name: string) => {
  await ensureMigrationDirectory();

  const timestamp = new Date().getTime();

  await fs
    .open(`migrations/${timestamp}-${name}-up.sql`, "w")
    .then((handle) => handle.close);
  await fs
    .open(`migrations/${timestamp}-${name}-down.sql`, "w")
    .then((handle) => handle.close);
};

const migrate = async (migrations: Migration[], client: PoolClient) => {
  for (const migration of migrations) {
    try {
      logger.info(
        `${migration.up ? "applying" : "reverting"} migration ${migration.name}`
      );
      await client.query("BEGIN");
      await client.query(migration.sql);
      if (migration.up) {
        await client.query(
          `INSERT INTO migration (id, name, applied) VALUES (${migration.id}, '${migration.name}', default)`
        );
      } else {
        await client.query(`DELETE FROM migration WHERE id = ${migration.id}`);
      }
      await client.query("COMMIT");
    } catch (reason) {
      if (reason instanceof Object) {
        logger.error(reason);
      }

      await client.query("ROLLBACK");
      break;
    }
  }
};

const cmdUp = async (options: Options) => {
  const localMigrations = await findLocalMigrations(true).then((ms) =>
    ms.filter((m) => m.up)
  );

  if (!options.dbname) {
    logger.error("Expected database name");
    return program.outputHelp({ error: true });
  }

  const pool = poolFromOptions(options);

  const records = await fetchMigrationRecords(pool);
  const recordSet = new Set(records.map((r) => `${r.id}-${r.name}`));

  await pool.end();

  const toRun = localMigrations
    .filter((m) => !recordSet.has(`${m.id}-${m.name}`))
    .sort((a, b) => {
      return a.id - b.id;
    });

  const pgPool = pgPoolFromOptions(options);

  const client = await pgPool.connect();

  logger.info(
    `running ${toRun.length} of ${localMigrations.length} migrations`
  );

  await migrate(toRun, client);
  client.release();

  await pgPool.end();
};

const cmdDown = async (options: Options) => {
  const localMigrations = await findLocalMigrations(false).then((ms) =>
    ms.filter((m) => !m.up)
  );

  if (!options.dbname) {
    logger.error("Expected database name");
    return program.outputHelp({ error: true });
  }

  const pool = poolFromOptions(options);

  const records = await fetchMigrationRecords(pool);
  const recordSet = new Set(records.map((r) => `${r.id}-${r.name}`));

  await pool.end();

  const toRun = localMigrations
    .filter((m) => recordSet.has(`${m.id}-${m.name}`))
    .sort((a, b) => {
      return b.id - a.id;
    });

  const pgPool = pgPoolFromOptions(options);

  const client = await pgPool.connect();

  logger.info(
    `reverting ${toRun.length} of ${localMigrations.length} migrations`
  );

  await migrate(toRun, client);
  client.release();

  await pgPool.end();
};

const cmdRevert = async (options: Options) => {
  if (!options.dbname) {
    logger.error("Expected database name");
    return program.outputHelp({ error: true });
  }

  const pool = poolFromOptions(options);
  const head = await fetchMigrationHeadRecord(pool);
  await pool.end();

  if (!head) {
    logger.info("Nothing to revert");
    return;
  }

  const localMigration = await readLocalMigration(head.id, head.name, false);

  const pgPool = pgPoolFromOptions(options);

  const client = await pgPool.connect();

  await migrate([localMigration], client);
  client.release();

  await pgPool.end();
};

const cmdStatus = async (options: Options): Promise<void> => {
  if (!options.dbname) {
    logger.error("Expected database name");
    return program.outputHelp({ error: true });
  }

  const pool = poolFromOptions(options);
  const records = await fetchMigrationRecords(pool);
  await pool.end();

  const localMigrations = await findLocalMigrations(true);

  const ids = new Set([
    ...records.map((r) => r.id),
    ...localMigrations.map((m) => m.id),
  ]);

  for (const id of ids.keys()) {
    const localMigration = localMigrations.find((l) => l.id === id);
    const record = records.find((r) => r.id === id);

    const name = record?.name ?? localMigration?.name;
    const applied = !!record?.applied;

    process.stdout.write(`${id}-${name} applied: ${applied}\n`);
  }

  return;
};

program.command("new").argument("<name>", "migration name").action(cmdNew);
program
  .command("up")
  .option("-h --host <host>")
  .option("-p --port <port>")
  .option("-d --dbname <name>")
  .action(cmdUp);
program
  .command("down")
  .option("-h --host <host>")
  .option("-p --port <port>")
  .option("-d --dbname <name>")
  .action(cmdDown);
program
  .command("revert")
  .option("-h --host <host>")
  .option("-p --port <port>")
  .option("-d --dbname <name>")
  .action(cmdRevert);
program
  .command("status")
  .option("-h --host <host>")
  .option("-p --port <port>")
  .option("-d --dbname <name>")
  .action(cmdStatus);

const main = async () => {
  await program.parseAsync();

  process.exit(0);
};

main().catch((reason) => {
  logger.error(reason);
  process.exit(-1);
});
