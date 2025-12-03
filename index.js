import "dotenv/config";
import express from "express";
import snowflake from "snowflake-sdk";
import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

function createConnection() {
  return snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USER,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
  });
}

async function runQuery(sql) {
  const connection = createConnection();
  return new Promise((resolve, reject) => {
    connection.connect(err => {
      if (err) return reject(err);
      connection.execute({
        sqlText: sql,
        complete: (err, _stmt, rows) => {
          connection.destroy(() => {});
          if (err) reject(err);
          else resolve(rows || []);
        }
      });
    });
  });
}

const server = new McpServer({
  name: "snowflake-mcp",
  version: "1.0.0"
});

const forbidden = /\b(UPDATE|DELETE|INSERT|MERGE|DROP|ALTER|TRUNCATE)\b/i;

server.tool(
  "run_query",
  "Run SELECT query on Snowflake",
  { sql: z.string() },
  async ({ sql }) => {
    if (forbidden.test(sql)) throw new Error("Only SELECT allowed");
    const rows = await runQuery(sql);
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(rows)
        }
      ]
    };
  }
);

const app = express();
app.use(express.json());

app.get("/health", (_req, res) => res.json({ ok: true }));

const transport = new StreamableHTTPServerTransport({
  sessionIdGenerator: undefined
});

app.post("/mcp", async (req, res) => {
  try {
    await transport.handleRequest(req, res, req.body);
  } catch (e) {
    console.error(e);
    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: "2.0",
        error: { code: -32603, message: "Internal error" },
        id: req.body?.id ?? null
      });
    }
  }
});

const port = process.env.PORT || 3000;

(async () => {
  await server.connect(transport);
  console.log("Registered tools:", Object.keys(server.tools));
  app.listen(port, () => console.log(`MCP server running on ${port}`));
})();
