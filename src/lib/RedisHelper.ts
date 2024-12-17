import Redis, { RedisOptions } from 'ioredis';

export class RedisHelper {
  private client: Redis; // Cliente padrão para operações gerais (get/set, filas, streams)
  private subscriber?: Redis; // Cliente dedicado para subscrição (Pub/Sub)

  constructor(url: string) {
    this.client = new Redis(url);
  }

  /**
   * Adiciona uma mensagem à fila.
   * @param queueName Nome da fila.
   * @param message Mensagem a ser adicionada.
   */
  async pushToQueue(queueName: string, message: string): Promise<void> {
    await this.client.rpush(queueName, message);
  }

  /**
   * Consome uma mensagem da fila de forma bloqueante.
   * @param queueName Nome da fila.
   * @param timeout (opcional) Tempo de espera em segundos.
   * @returns Mensagem consumida ou null.
   */
  async popFromQueue(queueName: string, timeout: number = 0): Promise<string | null> {
    const result = await this.client.blpop(queueName, timeout);
    return result ? result[1] : null;
  }

  // ==== Métodos para Streams ====

  /**
   * Adiciona uma mensagem ao stream.
   * @param streamName Nome do stream.
   * @param message Objeto contendo os campos e valores da mensagem.
   */
  async addToStream(streamName: string, message: Record<string, string>): Promise<void> {
    await this.client.xadd(streamName, '*', ...Object.entries(message).flat());
  }

  /**
   * Cria um grupo de consumidores para um stream.
   * @param streamName Nome do stream.
   * @param groupName Nome do grupo de consumidores.
   */
  async createConsumerGroup(streamName: string, groupName: string): Promise<void> {
    try {
      await this.client.xgroup('CREATE', streamName, groupName, '$', 'MKSTREAM');
    } catch (err: any) {
      if (!err.message.includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  /**
   * Lê mensagens de um grupo de consumidores.
   * @param streamName Nome do stream.
   * @param groupName Nome do grupo de consumidores.
   * @param consumerName Nome do consumidor.
   * @param count (opcional) Número máximo de mensagens a serem lidas.
   * @param block (opcional) Tempo de bloqueio em milissegundos.
   * @returns Lista de mensagens lidas.
   */
  async readFromStream(
    streamName: string,
    groupName: string,
    consumerName: string,
    count: number = 1,
    block: number = 0
  ): Promise<any[]> {
    const messages = await this.client.xreadgroup(
      'GROUP',
      groupName,
      consumerName,
      'COUNT',
      count,
      'BLOCK',
      block,
      'STREAMS',
      streamName,
      '>'
    );
    return messages || [];
  }

  // ==== Métodos de Limpeza ====

  /**
   * Fecha todas as conexões Redis.
   */
  async close(): Promise<void> {
    await this.client.quit();
    if (this.subscriber) {
      await this.subscriber.quit();
    }
  }
}
