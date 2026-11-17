import { Writer, SchemaRegistry, SCHEMA_TYPE_STRING } from "k6/x/kafka";

const BROKERS = (__ENV.BROKERS || "localhost:9092").split(",");
const TOPIC = __ENV.TOPIC || "users.in";

const PROFILES = {
  smoke: {
    smoke: {
      executor: "constant-arrival-rate",
      rate: 1,
      timeUnit: "1s",
      duration: "30s",
      preAllocatedVUs: 1,
      maxVUs: 5,
    },
  },
  load: {
    step_5rps: {
      executor: "constant-arrival-rate",
      rate: 5,
      timeUnit: "1s",
      duration: "5m",
      preAllocatedVUs: 10,
      maxVUs: 50,
    },
    step_10rps: {
      executor: "constant-arrival-rate",
      rate: 10,
      timeUnit: "1s",
      duration: "5m",
      startTime: "5m",
      preAllocatedVUs: 20,
      maxVUs: 50,
    },
  },
};

const profile = __ENV.PROFILE || "load";
if (!PROFILES[profile]) {
  throw new Error(`Unknown PROFILE=${profile}, expected one of: ${Object.keys(PROFILES).join(", ")}`);
}

export const options = {
  scenarios: PROFILES[profile],
};

const writer = new Writer({
  brokers: BROKERS,
  topic: TOPIC,
  batchSize: 1,
  requiredAcks: -1,
});

const schemaRegistry = new SchemaRegistry();

function asBytes(text) {
  return schemaRegistry.serialize({ data: text, schemaType: SCHEMA_TYPE_STRING });
}

export default function () {
  const userId = String(Math.floor(Math.random() * 1000000) + 1);

  writer.produce({
    messages: [
      {
        key: asBytes(userId),
        value: asBytes(JSON.stringify({ userId: userId })),
      },
    ],
  });
}

export function teardown() {
  writer.close();
}
