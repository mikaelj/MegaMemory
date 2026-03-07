import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { KnowledgeDB } from "../db.js";
import fs from "fs";
import path from "path";
import os from "os";

let db: KnowledgeDB;
let dbPath: string;
let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "megamemory-test-"));
  dbPath = path.join(tmpDir, "knowledge.db");
  db = new KnowledgeDB(dbPath);
});

afterEach(() => {
  db.close();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe("database configuration", () => {
  it("should have WAL mode enabled", () => {
    const pragmaResult = db["db"].pragma("journal_mode", { simple: true });
    const mode =
      typeof pragmaResult === "object" && pragmaResult !== null
        ? (pragmaResult as Record<string, unknown>).journal_mode
        : pragmaResult;
    expect(mode).toBe("wal");
  });

  it("should have auto-checkpoint configured", () => {
    const pragmaResult = db["db"].pragma("wal_autocheckpoint", { simple: true });
    const checkpoint =
      typeof pragmaResult === "object" && pragmaResult !== null
        ? (pragmaResult as Record<string, unknown>).wal_autocheckpoint
        : pragmaResult;
    expect(checkpoint as number).toBeGreaterThan(0);
  });
});
