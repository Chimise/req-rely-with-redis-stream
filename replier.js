import {RedisReply} from "./index.js";

const [,, consumer] = process.argv;

async function main() {
  const reply = new RedisReply("results_stream", 'results_group', consumer);
  
  reply.onMessage((req) => {
    console.log("Request received", req);
    return { sum: req.a + req.b };
  });
}

main().catch((err) => console.error(err));
