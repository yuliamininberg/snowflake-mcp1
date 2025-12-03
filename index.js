import "dotenv/config";
import express from "express";
import snowflake from "snowflake-sdk";
import { z } from "zod";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

//
// 1. SNOWFLAKE CONNECTION
//
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
    console.log("Connecting to Snowflake...");
    connection.connect(err => {
      if (err) {
        console.error("❌ Snowflake connection error:", err);
        return reject(err);
      }

      console.log("Connected. Executing SQL:", sql);
      connection.execute({
        sqlText: sql,
        complete: (err, stmt, rows) => {
          connection.destroy(() => {});
          if (err) {
            console.error("❌ Snowflake query error:", err);
            reject(err);
          } else {
            console.log("Query returned rows:", rows?.length || 0);
            resolve(rows || []);
          }
        }
      });
    });
  });
}

//
// 2. MCP SERVER + TOOL REGISTRATION
//
console.log("Creating MCP server...");
const server = new McpServer({
  name: "snowflake-mcp",
  version: "1.0.0"
});

const forbidden = /\b(UPDATE|DELETE|INSERT|MERGE|DROP|ALTER|TRUNCATE)\b/i;

// 🔥 HIGH-DETAIL LOGGING FOR TOOL REGISTRATION
console.log("Registering MCP tool: run_query...");
server.tool(
  "run_query",
  "Execute a SQL SELECT query on Snowflake",
  {
    sql: z.string()
  },
  async ({ sql }) => {
    console.log("🔥 run_query tool invoked! SQL =", sql);

    if (forbidden.test(sql)) {
      console.log("❌ Blocked non-SELECT query");
      throw new Error("Only SELECT queries are allowed.");
    }

    const rows = await runQuery(sql);

    return {
      content: [{
        type: "text",
        text: JSON.stringify(rows)
      }]
    };
  }
);
console.log("✔ MCP tool registered: run_query");

//
// 3. EXPRESS SERVER + MCP TRANSPORT
//
const app = express();
app.use(express.json());

// Health check
app.get("/health", (req, res) => {
  res.json({ ok: true });
});

// Create the MCP transport (stateless mode)
console.log("Initializing StreamableHTTPServerTransport...");
const transport = new StreamableHTTPServerTransport({
  sessionIdGenerator: undefined
});

// This is the REAL /mcp endpoint
app.post("/mcp", async (req, res) => {
  console.log("📥 Incoming MCP request:", req.body);

  try {
    await transport.handleRequest(req, res, req.body);
  } catch (error) {
    console.error("❌ Error handling MCP request:", error);

    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: "2.0",
        error: {
          code: -32603,
          message: "Internal Server Error"
        },
        id: req.body?.id ?? null
      });
    }
  }
});

//
// 4. START SERVER
//
const port = process.env.PORT || 3000;

(async () => {
  try {
    console.log("Connecting MCP server to transport...");
    await server.connect(transport);
    console.log("✔ MCP transport connected.");

    app.listen(port, () => {
      console.log(`🚀 MCP server running on port ${port}`);
    });
  } catch (err) {
    console.error("❌ MCP server failed to start:", err);
    process.exit(1);
  }
})();
