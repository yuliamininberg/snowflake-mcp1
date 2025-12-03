import "dotenv/config";
import express from "express";
import snowflake from "snowflake-sdk";
import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

// ===== 1. Snowflake connection (password auth) =====
function createConnection() {
  return snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USER,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
  });
}

async function runQuery(sql) {
  const connection = createConnection();

  return new Promise((resolve, reject) => {
    connection.connect((err) => {
      if (err) {
        console.error("❌ Snowflake connect error:", err);
        return reject(err);
      }

      connection.execute({
        sqlText: sql,
        complete: (err, _stmt, rows) => {
          connection.destroy(() => {});
          if (err) {
            console.error("❌ Snowflake query error:", err);
            reject(err);
          } else {
            resolve(rows || []);
          }
        },
      });
    });
  });
}

// ===== 2. MCP server definition =====
const server = new McpServer({
  name: "snowflake-mcp",
  version: "1.0.0",
});

// Only allow SELECT-style queries for safety
const forbidden = /\b(UPDATE|DELETE|INSERT|MERGE|DROP|ALTER|TRUNCATE)\b/i;

server.tool(
  "run_query",
  "Run a read-only SELECT query on Snowflake",
  {
    sql: z.string().describe("Snowflake SELECT statement"),
  },
  async ({ sql }) => {
    if (forbidden.test(sql)) {
      throw new Error("Only read-only SELECT queries are allowed.");
    }

    const rows = await runQuery(sql);

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(rows),
        },
      ],
    };
  }
);

// ===== 3. Express + Streamable HTTP transport =====
const app = express();
app.use(express.json());

// Simple health check
app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

// Create the MCP transport (NO app/endpoint here)
const transport = new StreamableHTTPServerTransport({
  sessionIdGenerator: undefined, // stateless server
});

// Wire MCP server to the transport
async function setupMcp() {
  await server.connect(transport);
}

// The actual /mcp route that handles POSTs
app.post("/mcp", async (req, res) => {
  console.log("📥 Received MCP request:", req.body);
  try {
    await transport.handleRequest(req, res, req.body);
  } catch (error) {
    console.error("❌ Error handling MCP request:", error);
    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: "2.0",
        error: {
          code: -32603,
          message: "Internal server error",
        },
        id: req.body?.id ?? null,
      });
    }
  }
});

// Start everything
const port = process.env.PORT || 3000;

setupMcp()
  .then(() => {
    app.listen(port, () => {
      console.log(`✅ MCP server listening on port ${port}`);
    });
  })
  .catch((err) => {
    console.error("❌ Failed to start MCP server:", err);
    process.exit(1);
  });
