import { createClient, RedisClientType } from 'redis';

export class RedisHelper2 {
  private client: RedisClientType; // Cliente padrão para operações gerais (get/set, filas, streams)
  private subscriber?: RedisClientType; // Cliente dedicado para subscrição (Pub/Sub)

  constructor(url: string) {
    this.client = createClient({ url });
  }

  // ==== Métodos Simples: Get/Set ====

  /**
   * Define um valor no Redis.
   * @param key Chave do valor.
   * @param value Valor a ser armazenado.
   * @param expiration (opcional) Tempo de expiração em segundos.
   */
  async set(key: string, value: string, expiration?: number): Promise<void> {
    if (expiration) {
      await this.client.set(key, value, { EX: expiration });
    } else {
      await this.client.set(key, value);
    }
  }

  /**
   * Obtém um valor do Redis.
   * @param key Chave a ser consultada.
   * @returns Valor armazenado ou null.
   */
  async get(key: string): Promise<string | null> {
    return await this.client.get(key);
  }

  // ==== Métodos Pub/Sub ====

  /**
   * Publica uma mensagem em um canal.
   * @param channel Nome do canal.
   * @param message Mensagem a ser publicada.
   */
  async publish(channel: string, message: string): Promise<void> {
    await this.client.publish(channel, message);
  }

  /**
   * Inscreve-se em um canal para receber mensagens.
   * @param channel Nome do canal.
   * @param callback Função de callback chamada com cada mensagem recebida.
   */
  async subscribe(channel: string, callback: (message: string) => void): Promise<void> {
    if (!this.subscriber) {
      this.subscriber = createClient({ url: this.client?.options?.url });
      await this.subscriber.connect();
    }

    await this.subscriber.subscribe(channel, (message) => {
      callback(message);
    });
  }

  // ==== Métodos para Filas (Lists) ====

  /**
   * Adiciona uma mensagem à fila.
   * @param queueName Nome da fila.
   * @param message Mensagem a ser adicionada.
   */
  async pushToQueue(queueName: string, message: string): Promise<void> {
    await this.client.rPush(queueName, message);
  }

  /**
   * Consome uma mensagem da fila de forma bloqueante.
   * @param queueName Nome da fila.
   * @param timeout (opcional) Tempo de espera em segundos.
   * @returns Mensagem consumida ou null.
   */
  async popFromQueue(queueName: string, timeout: number = 0): Promise<string | null> {
    const result: { key: string; element: string } | null = await this.client.blPop(queueName, timeout);
    return result ? result.element : null;
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
