import {
  describeStorage,
  openStorageFromEnvironment,
  selectStorage,
} from "../src/storage-factory.js";
import { gitlabScenario } from "../test/fixtures.js";

const selection = selectStorage();
const store = await openStorageFromEnvironment();
try {
  const results = [];
  for (const observation of gitlabScenario()) {
    results.push(await store.ingest(observation));
  }
  const accepted = results.filter((result) => result.status === "accepted").length;
  const duplicates = results.filter((result) => result.status === "duplicate").length;
  const signals = results.flatMap((result) => result.generated_signals);
  console.log(
    JSON.stringify(
      {
        storage: describeStorage(selection),
        accepted,
        duplicates,
        generated_signals: signals,
      },
      null,
      2,
    ),
  );
} finally {
  await store.close();
}
