import { runStorageConformance } from "./conformance.js";
import { SqliteStorage } from "../src/storage-sqlite.js";

runStorageConformance({
  name: "sqlite",
  create: async () => {
    const storage = SqliteStorage.open(":memory:");
    return {
      storage,
      dispose: async () => {
        await storage.close();
      },
    };
  },
});
