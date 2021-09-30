import { program } from "commander";
import { promises } from "fs";
import { createPool, DatabasePoolType, sql } from "slonik";
import { Pool } from "pg";

const fs = promises;

type Migration = {
  timestamp: number;
  name: string;
  sql: string;
  up: boolean;
};

type MigrationRecord = {
  timestamp: number;
  name: string;
  applied: Date;
};

type Options = {
  host: string;
  port: number;
  username: string;
  password: string | undefined;
  dbname: string;
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
      timestamp int8 NOT NULL,
      name text NOT NULL,
      applied timestamptz DEFAULT now() NOT NULL,
      CONSTRAINT migration_pk PRIMARY KEY (timestamp, name)
    );
  `);
};

const fetchMigrationRecords = async (
  pool: DatabasePoolType
): Promise<readonly MigrationRecord[]> => {
  await ensureMigrationTable(pool);

  return pool.any<MigrationRecord>(sql`SELECT * FROM migration`);
};

const readLocalMigrations = async (): Promise<Migration[]> => {
  await ensureMigrationDirectory();
  const matches: RegExpMatchArray[] = await fs
    .readdir("migrations")
    .then((contents) =>
      contents.map((item) =>
        item.match(
          /^(?<timestamp>\d{13})-(?<name>.*)-(?<type>up\.sql|down\.sql)$/
        )
      )
    )
    .then((results) => results.filter<RegExpMatchArray>(notNull));

  const migrations: Migration[] = [];

  for (const { groups } of matches) {
    if (!groups) {
      throw new Error("Parse error");
    }

    const { timestamp, name, type } = groups;

    const sqlSrc = await fs
      .readFile(`migrations/${timestamp}-${name}-${type}`)
      .then((buffer) => buffer.toString("utf-8"));

    migrations.push({
      timestamp: parseInt(timestamp),
      name,
      sql: sqlSrc,
      up: type === "up.sql",
    });
  }

  return migrations;
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

const cmdUp = async (cliOptions: Partial<Options>) => {
  const localMigrations = await readLocalMigrations().then((ms) =>
    ms.filter((m) => m.up)
  );

  if (!cliOptions.dbname) {
    return program.outputHelp({ error: true });
  }

  const options: Options = { ...defaultOptions, ...cliOptions } as Options;

  const pool = poolFromOptions(options);

  const records = await fetchMigrationRecords(pool);
  const recordSet = new Set(records.map((r) => `${r.timestamp}-${r.name}`));

  await pool.end();

  const toRun = localMigrations
    .filter((m) => !recordSet.has(`${m.timestamp}-${m.name}`))
    .sort((a, b) => {
      return a.timestamp - b.timestamp;
    });

  const pgPool = pgPoolFromOptions(options);

  const client = await pgPool.connect();

  for (const run of toRun) {
    try {
      await client.query("BEGIN");
      await client.query(run.sql);
      await client.query(
        `INSERT INTO migration (timestamp, name, applied) VALUES (${run.timestamp}, '${run.name}', default)`
      );
      await client.query("COMMIT");
    } catch (reason) {
      client.query("ROLLBACK");
      reason instanceof Error ? client.release(reason) : client.release(true);
      break;
    }
  }

  await pgPool.end();
};

program.command("new").argument("<name>", "migration name").action(cmdNew);
program
  .command("up")
  .option("-h --host <host>")
  .option("-p --port <port>")
  .option("-d --dbname <name>")
  .action(cmdUp);

const main = async () => {
  await program.parseAsync();
};

main().catch((reason) => {
  process.stderr.write(`${reason}\n`);
  process.exit(-1);
});
