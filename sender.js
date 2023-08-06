import { RedisRequest } from "./index.js";

const delay = (time) =>
  new Promise((res) => {
    setTimeout(() => {
      res();
    }, time);
  });



async function main() {
  const request = new RedisRequest('results_stream', 'sender_stream');

  async function sendRandomRequest() {
    const a = Math.round(Math.random() * 100);
    const b = Math.round(Math.random() * 100);
    const reply = await request.send({ a, b });
    console.log(`${a} + ${b} = ${reply.sum}`);
  }

  for (let i = 0; i < 20; i++) {
    await sendRandomRequest();
    await delay(1000);
  }

  request.close();
}

main().catch((err) => console.error(err));
