import { RedisHelper } from "./lib/RedisHelper";
import './master'

const redis = new RedisHelper('redis://localhost:6379');

setInterval(async () => {
  const message = `Message ${Date.now()}`;
  await redis.pushToQueue('task-queue', message);
  console.log(`[Master] Pushed: ${message}`);
}, 2000);
