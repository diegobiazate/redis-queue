import { RedisHelper } from './lib/RedisHelper';
const redis = new RedisHelper('redis://localhost:6379');

(async () => {
  while (true) {
    const task = await redis.popFromQueue('task-queue');
    if (task) {
      console.log(`[Worker ${process.pid}] Processing: ${task}`);
      await new Promise((resolve) => setTimeout(resolve, 1000)); // Simula processamento
      console.log(`[Worker ${process.pid}] Done: ${task}`);
    }
  }
})();
