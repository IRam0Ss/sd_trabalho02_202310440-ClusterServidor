/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 15/10/2026
* Nome...............: NoCluster
* Funcao.............: Orquestrador da maquina de estados do nó no cluster, eleicao sem preempcao (D1) e ciclo de vida.
*************************************************************** */

package cluster;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import controller.GerenciadorGrupos;
import controller.Operacao;
import controller.Resultado;
import model.NotificadorClientes;
import model.Servidor;
import utils.Protocolo;

/**
 * Controla o papel do nó (ATIVO, STANDBY, SINCRONIZANDO, CANDIDATO), a eleicao de lideres,
 * os heartbeats periodicos e a ativacao/desativacao dinamica das portas de atendimento.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 1.0
 */
public class NoCluster implements CanalCluster.ReceptorMensagemCluster {

  private final ConfigCluster config;
  private final GerenciadorGrupos gerenciador;
  private final VisaoCluster visao;
  private final CanalCluster canal;
  private final Replicador replicador;
  private final Servidor servidor;

  private volatile String papel = "INICIANDO"; // INICIANDO, SINCRONIZANDO, STANDBY, CANDIDATO, ATIVO
  private volatile long epoca = 0;
  private volatile int idLiderAtivo = -1;
  private volatile boolean executando = true;

  // Latch de votacao durante eleicao
  private CountDownLatch latchVotos;
  private final AtomicBoolean eleicaoEmAndamento = new AtomicBoolean(false);
  private final long inicioMs = System.currentTimeMillis();

  public NoCluster(ConfigCluster config, GerenciadorGrupos gerenciador) {
    this.config = config;
    this.gerenciador = gerenciador;
    this.visao = new VisaoCluster(config);
    this.canal = new CanalCluster(config, visao, this);
    this.replicador = new Replicador(config.getMeuId(), gerenciador, canal, visao);
    this.servidor = new Servidor(gerenciador);
  }

  public int getMeuId() {
    return config.getMeuId();
  }

  public String getPapel() {
    return papel;
  }

  public long getEpoca() {
    return epoca;
  }

  public int getIdLiderAtivo() {
    return idLiderAtivo;
  }

  public GerenciadorGrupos getGerenciador() {
    return gerenciador;
  }

  public VisaoCluster getVisao() {
    return visao;
  }

  public Replicador getReplicador() {
    return replicador;
  }

  public CanalCluster getCanal() {
    return canal;
  }

  /**
   * Inicia o canal de cluster, descobre lideres e dispara os temporizadores.
   */
  public void iniciar() {
    canal.iniciar();

    // Loop de heartbeat e liveness (a cada 1000 ms)
    Thread threadHeartbeat = new Thread(() -> {
      while (executando) {
        try {
          enviarHeartbeat();
          boolean ativoCaiu = visao.verificarTimeouts(3000);
          if (!ativoCaiu && idLiderAtivo > 0 && idLiderAtivo != config.getMeuId()) {
            VisaoCluster.ParEstado liderSt = visao.getPar(idLiderAtivo);
            if (liderSt != null && (!liderSt.conectado || "DOWN".equalsIgnoreCase(liderSt.estado))) {
              System.out.println("[ELEICAO] [WARNING] Lider ativo conhecido (Nó " + idLiderAtivo + ") caiu ou esta inacessivel.");
              ativoCaiu = true;
            }
          }

          if (ativoCaiu && !"ATIVO".equalsIgnoreCase(papel)) {
            System.out.println("[ELEICAO] [WARNING] Disparando eleicao para eleger novo lider...");
            int antigoLider = this.idLiderAtivo;
            this.idLiderAtivo = -1;
            if (antigoLider > 0) {
              visao.marcarDown(antigoLider);
            }
            dispararEleicao();
          } else if (idLiderAtivo <= 0 && "STANDBY".equalsIgnoreCase(papel)) {
            dispararEleicao();
          }

          // Se estiver iniciando e apos timeout nenhum lider foi detectado
          if ("INICIANDO".equalsIgnoreCase(papel)) {
            avaliarInicializacao();
          }

          Thread.sleep(1000);
        } catch (InterruptedException e) {
          break;
        } catch (Exception e) {
          System.err.println("[CLUSTER] [ERROR] Falha no loop de heartbeat: " + e.getMessage());
        }
      }
    }, "Thread-NoCluster-Heartbeat");
    threadHeartbeat.setDaemon(true);
    threadHeartbeat.start();
  }

  private void enviarHeartbeat() {
    MensagemCluster hb = new MensagemCluster(TipoMensagemCluster.HEARTBEAT, config.getMeuId());
    hb.setEpoca(epoca);
    hb.setVersao(gerenciador.getVersao());
    hb.setHash(gerenciador.calcularHash());
    hb.setEstadoOrigem(papel);
    hb.setIdAtivoConhecido(idLiderAtivo);

    ConfigCluster.NoInfo info = config.getMeuNoInfo();
    hb.setHostOrigem(info.host);
    hb.setPortaClusterOrigem(info.portaCluster);
    hb.setPortaTcpServico(info.portaTcp);
    hb.setPortaUdpServico(info.portaUdp);

    canal.broadcast(hb);
  }

  private void avaliarInicializacao() {
    long decorrido = System.currentTimeMillis() - inicioMs;
    // Aguarda pelo menos 1500ms para conexoes do cluster se estabelecerem e heartbeats circularem
    if (decorrido < 1500) {
      return;
    }

    // Verifica se ha algum lider ativo nos pares
    int ativoConhecido = visao.getIdAtivoAtual();
    if (ativoConhecido > 0) {
      System.out.println("[CLUSTER] [INFO] Lider ativo detectado no Nó " + ativoConhecido + ". Solicitando sincronizacao...");
      this.idLiderAtivo = ativoConhecido;
      this.papel = "SINCRONIZANDO";
      MensagemCluster pedido = new MensagemCluster(TipoMensagemCluster.PEDIDO_SYNC, config.getMeuId());
      pedido.setVersao(gerenciador.getVersao());
      canal.enviar(ativoConhecido, pedido);
    } else {
      // Se algum nó com menor ID existir e estiver conectando/vivo, aguarda por ele ou fica em STANDBY
      int meuId = config.getMeuId();
      boolean temMenorIdEsperado = false;
      for (ConfigCluster.NoInfo outro : config.getOutrosNos()) {
        if (outro.id < meuId) {
          VisaoCluster.ParEstado st = visao.getPar(outro.id);
          // Se o par com menor id estiver conectado ou o tempo decorrido for menor que 3000ms, da prioridade a ele
          if (st != null && (st.conectado || decorrido < 3000)) {
            temMenorIdEsperado = true;
            break;
          }
        }
      }

      if (temMenorIdEsperado) {
        return; // Aguarda o nó de menor ID se apresentar ou assumir
      }

      System.out.println("[CLUSTER] [INFO] Nenhum lider ativo encontrado. Assumindo lideranca inicial (Nó " + meuId + ")...");
      assumirComoAtivo();
    }
  }

  /**
   * Dispara o processo de eleicao sem preempcao.
   */
  public synchronized void dispararEleicao() {
    if (!eleicaoEmAndamento.compareAndSet(false, true)) {
      return;
    }

    new Thread(() -> {
      try {
        int meuId = config.getMeuId();
        // Escalonamento deterministico por ID para evitar tempestades concorrentes de eleicao
        int delayMs = Math.max(0, (meuId - 1) * 80);
        if (delayMs > 0) {
          Thread.sleep(delayMs);
        }

        // Verifica se algum lider ja foi anunciado durante a espera
        if (idLiderAtivo > 0 && idLiderAtivo != meuId) {
          VisaoCluster.ParEstado liderAtual = visao.getPar(idLiderAtivo);
          if (liderAtual != null && liderAtual.isVivo()) {
            return;
          }
        }

        System.out.println("[ELEICAO] [INFO] Avaliando candidatos elegiveis vivos...");
        List<VisaoCluster.ParEstado> elegiveis = visao.getParesElegiveis();

        long minhaVersao = gerenciador.getVersao();
        boolean souMelhor = true;

        for (VisaoCluster.ParEstado p : elegiveis) {
          if (p.id == meuId) continue;
          if (p.versao > minhaVersao) {
            souMelhor = false;
            break;
          } else if (p.versao == minhaVersao && p.id < meuId) {
            souMelhor = false;
            break;
          }
        }

        if (souMelhor) {
          this.papel = "CANDIDATO";
          this.epoca = Math.max(this.epoca, visao.getMaiorEpocaConhecida()) + 1;
          System.out.println("[ELEICAO] [INFO] Sou o melhor candidato (Versao: " + minhaVersao
              + ", ID: " + meuId + "). Enviando PROPOSTA na epoca " + epoca + "...");

          List<VisaoCluster.ParEstado> alvos = visao.getParesVivos();
          alvos.removeIf(p -> p.id == meuId || !p.conectado || "DOWN".equalsIgnoreCase(p.estado));

          if (alvos.isEmpty()) {
            // Unico sobrevivente: assume imediatamente
            System.out.println("[ELEICAO] [INFO] Unico nó sobrevivente ativo no cluster. Assumindo lideranca.");
            assumirComoAtivo();
            return;
          }

          latchVotos = new CountDownLatch(alvos.size());
          MensagemCluster proposta = new MensagemCluster(TipoMensagemCluster.PROPOSTA, meuId);
          proposta.setEpoca(epoca);
          proposta.setVersao(minhaVersao);

          for (VisaoCluster.ParEstado p : alvos) {
            canal.enviar(p.id, proposta);
          }

          boolean venceu = latchVotos.await(2000, TimeUnit.MILLISECONDS);
          if (venceu || latchVotos.getCount() == 0) {
            if ("CANDIDATO".equalsIgnoreCase(this.papel)) {
              System.out.println("[ELEICAO] [INFO] Votos obtidos com sucesso! Assumindo lideranca.");
              assumirComoAtivo();
            }
          } else {
            System.out.println("[ELEICAO] [WARNING] Timeout de votacao. Voltando para STANDBY.");
            if ("CANDIDATO".equalsIgnoreCase(this.papel)) {
              this.papel = "STANDBY";
            }
          }
        } else {
          System.out.println("[ELEICAO] [INFO] Outro nó tem maior prioridade para lideranca. Aguardando COORDENADOR...");
          this.papel = "STANDBY";
        }
      } catch (Exception e) {
        System.err.println("[ELEICAO] [ERROR] Falha na eleicao: " + e.getMessage());
      } finally {
        eleicaoEmAndamento.set(false);
      }
    }, "Thread-Eleicao").start();
  }

  /**
   * Promove o nó para ATIVO: abre portas de atendimento e notifica os clientes.
   */
  public synchronized void assumirComoAtivo() {
    this.papel = "ATIVO";
    this.idLiderAtivo = config.getMeuId();

    ConfigCluster.NoInfo info = config.getMeuNoInfo();
    boolean incluirLegado = (config.getMeuId() == 1);
    servidor.iniciarServicos(info.portaTcp, info.portaUdp, info.portaDiscovery, incluirLegado, config.getMeuId());

    // Envia COORDENADOR para todos os pares
    MensagemCluster coord = new MensagemCluster(TipoMensagemCluster.COORDENADOR, config.getMeuId());
    coord.setEpoca(epoca);
    coord.setVersao(gerenciador.getVersao());
    coord.setPortaTcpServico(info.portaTcp);
    coord.setPortaUdpServico(info.portaUdp);
    canal.broadcast(coord);

    // Dispara notificacao SERVER_CHANGED para todos os clientes registrados
    NotificadorClientes.notificarServerChanged(
        gerenciador.getTodosUsuariosAtivos(),
        info.host,
        info.portaTcp,
        info.portaUdp,
        epoca,
        config.getMeuId()
    );

    // Conecta replicacao sincrona para todas as operacoes recebidas dos clientes (D2)
    gerenciador.setListenerReplicacao(op -> replicador.executar(op, this.epoca));
  }

  /**
   * Rebaixa o nó para STANDBY: fecha portas de servico.
   */
  public synchronized void rebaixarParaStandby(int novoLider, long novaEpoca) {
    gerenciador.setListenerReplicacao(null);
    if ("ATIVO".equalsIgnoreCase(this.papel)) {
      System.out.println("[CLUSTER] [WARNING] Rebaixando de ATIVO para STANDBY. Novo lider: Nó " + novoLider);
      servidor.pararServicos();
    }
    this.papel = "STANDBY";
    this.idLiderAtivo = novoLider;
    this.epoca = Math.max(this.epoca, novaEpoca);
  }

  // =========================================================================
  // PROCESSAMENTO DE MENSAGENS RECEBIDAS DO CLUSTER
  // =========================================================================

  @Override
  public void onMensagemRecebida(MensagemCluster msg) {
    if (msg == null) return;

    // Garante que novos nós descobertos dinamicamente sejam registrados
    if (msg.getIdOrigem() > 0 && msg.getIdOrigem() != config.getMeuId()) {
      String hostOrig = (msg.getHostOrigem() != null && !msg.getHostOrigem().isEmpty()) ? msg.getHostOrigem() : "127.0.0.1";
      int pCluster = (msg.getPortaClusterOrigem() > 0) ? msg.getPortaClusterOrigem() : (9000 + msg.getIdOrigem());
      int pTcp = (msg.getPortaTcpServico() > 0) ? msg.getPortaTcpServico() : (6789 + 10 * (msg.getIdOrigem() - 1));
      int pUdp = (msg.getPortaUdpServico() > 0) ? msg.getPortaUdpServico() : (7777 + 10 * (msg.getIdOrigem() - 1));

      ConfigCluster.NoInfo infoPar = new ConfigCluster.NoInfo(
          msg.getIdOrigem(), hostOrig, pCluster, pTcp, pUdp, Protocolo.PORTA_DISCOVERY
      );
      config.registrarParDinamico(infoPar);
      visao.adicionarParSeAusente(infoPar);
      canal.adicionarParSeAusente(msg.getIdOrigem(), hostOrig, pCluster);
    }

    // Atualiza status do par
    visao.atualizarHeartbeat(
        msg.getIdOrigem(),
        msg.getEstadoOrigem() != null ? msg.getEstadoOrigem() : "STANDBY",
        msg.getEpoca(),
        msg.getVersao(),
        msg.getHash(),
        msg.getPortaTcpServico(),
        msg.getPortaUdpServico()
    );

    // Se receber qualquer mensagem com epoca maior, rebaixa-se se era lider
    if (msg.getEpoca() > this.epoca && "ATIVO".equalsIgnoreCase(this.papel)) {
      rebaixarParaStandby(msg.getIdOrigem(), msg.getEpoca());
    }

    switch (msg.getTipo()) {
      case HELLO:
      case HEARTBEAT:
        if ("ATIVO".equalsIgnoreCase(msg.getEstadoOrigem())) {
          if ("ATIVO".equalsIgnoreCase(this.papel)) {
            if (msg.getIdOrigem() < config.getMeuId() || msg.getEpoca() > this.epoca) {
              System.out.println("[CLUSTER] [CONFLITO] Cedendo lideranca ao Nó " + msg.getIdOrigem());
              rebaixarParaStandby(msg.getIdOrigem(), msg.getEpoca());
              this.papel = "SINCRONIZANDO";
              MensagemCluster pedido = new MensagemCluster(TipoMensagemCluster.PEDIDO_SYNC, config.getMeuId());
              pedido.setVersao(gerenciador.getVersao());
              canal.enviar(msg.getIdOrigem(), pedido);
            }
          } else {
            this.idLiderAtivo = msg.getIdOrigem();
            if ("INICIANDO".equalsIgnoreCase(this.papel)) {
              this.papel = "SINCRONIZANDO";
              MensagemCluster pedido = new MensagemCluster(TipoMensagemCluster.PEDIDO_SYNC, config.getMeuId());
              pedido.setVersao(gerenciador.getVersao());
              canal.enviar(msg.getIdOrigem(), pedido);
            }
          }
        }
        break;

      case REPLICAR:
        if (!"ATIVO".equalsIgnoreCase(this.papel)) {
          replicador.processarReplicacaoRecebida(msg);
        }
        break;

      case ACK:
        if ("ATIVO".equalsIgnoreCase(this.papel)) {
          replicador.processarAck(msg.getIdOrigem(), msg.getVersao());
        }
        break;

      case PEDIDO_SYNC:
        if ("ATIVO".equalsIgnoreCase(this.papel)) {
          replicador.enviarSnapshot(msg.getIdOrigem(), epoca);
        }
        break;

      case SNAPSHOT:
        if (!"ATIVO".equalsIgnoreCase(this.papel)) {
          if (msg.getPayload() instanceof byte[]) {
            byte[] dados = (byte[]) msg.getPayload();
            replicador.instalarSnapshot(dados, msg.getEpoca(), msg.getVersao());
            this.epoca = msg.getEpoca();
            this.papel = "STANDBY"; // Nó sincronizado entra como STANDBY (D1)
            this.idLiderAtivo = msg.getIdOrigem();

            MensagemCluster ok = new MensagemCluster(TipoMensagemCluster.SYNC_OK, config.getMeuId());
            ok.setVersao(gerenciador.getVersao());
            canal.enviar(msg.getIdOrigem(), ok);
            System.out.println("[CLUSTER] [INFO] Sincronizacao concluida. Nó operando em STANDBY.");
          }
        }
        break;

      case SYNC_OK:
        System.out.println("[CLUSTER] [INFO] Nó " + msg.getIdOrigem() + " sincronizou com sucesso e esta em STANDBY.");
        break;

      case PROPOSTA:
        tratarProposta(msg);
        break;

      case VOTO:
        if (latchVotos != null && msg.getEpoca() == this.epoca) {
          if (Boolean.TRUE.equals(msg.getPayload())) {
            latchVotos.countDown();
          }
        }
        break;

      case COORDENADOR:
        if (msg.getEpoca() >= this.epoca) {
          System.out.println("[CLUSTER] [INFO] Novo lider anunciado: Nó " + msg.getIdOrigem() + " (Epoca: " + msg.getEpoca() + ")");
          this.idLiderAtivo = msg.getIdOrigem();
          this.epoca = msg.getEpoca();
          if (!"ATIVO".equalsIgnoreCase(this.papel) || msg.getIdOrigem() != config.getMeuId()) {
            this.papel = "STANDBY";
          }
        } else {
          System.out.println("[CLUSTER] [WARNING] Ignorando COORDENADOR obsoleto do Nó " + msg.getIdOrigem()
              + " (Epoca " + msg.getEpoca() + " < Minha " + this.epoca + ")");
        }
        break;

      case RENUNCIA:
        System.out.println("[CLUSTER] [INFO] Lider Nó " + msg.getIdOrigem() + " renunciou graciosamente. Disparando eleicao imediata.");
        visao.marcarDown(msg.getIdOrigem());
        this.idLiderAtivo = -1;
        dispararEleicao();
        break;

      case SAINDO:
        visao.marcarDown(msg.getIdOrigem());
        break;

      default:
        break;
    }
  }

  private synchronized void tratarProposta(MensagemCluster msg) {
    MensagemCluster voto = new MensagemCluster(TipoMensagemCluster.VOTO, config.getMeuId());
    voto.setEpoca(msg.getEpoca());

    long minhaVersao = gerenciador.getVersao();
    int meuId = config.getMeuId();

    boolean aceita = true;

    // 1. Proposta com epoca desatualizada: rejeitada
    if (msg.getEpoca() < this.epoca) {
      aceita = false;
    }
    // 2. Se ja sou ATIVO e a proposta nao tem epoca estritamente maior: rejeitada
    else if ("ATIVO".equalsIgnoreCase(this.papel) && msg.getEpoca() <= this.epoca) {
      aceita = false;
    }
    // 3. Se os dados do candidato forem mais antigos que os meus: rejeitada
    else if (msg.getVersao() < minhaVersao) {
      aceita = false;
    }
    // 4. Se a versao for igual: desempate por menor ID
    else if (msg.getVersao() == minhaVersao) {
      if (msg.getIdOrigem() > meuId && ("CANDIDATO".equalsIgnoreCase(this.papel) || "ATIVO".equalsIgnoreCase(this.papel))) {
        aceita = false;
      }
    }

    if (aceita) {
      this.epoca = Math.max(this.epoca, msg.getEpoca());
      if ("ATIVO".equalsIgnoreCase(this.papel)) {
        rebaixarParaStandby(msg.getIdOrigem(), msg.getEpoca());
      } else if ("CANDIDATO".equalsIgnoreCase(this.papel)) {
        this.papel = "STANDBY";
      }
    }

    voto.setPayload(aceita);
    canal.enviar(msg.getIdOrigem(), voto);
  }

  /**
   * Encerramento gracioso do nó com aviso imediato aos pares (RENUNCIA / SAINDO).
   */
  public void encerrar() {
    this.executando = false;
    if ("ATIVO".equalsIgnoreCase(this.papel)) {
      System.out.println("[CLUSTER] [INFO] Enviando RENUNCIA aos pares antes de encerrar...");
      MensagemCluster renuncia = new MensagemCluster(TipoMensagemCluster.RENUNCIA, config.getMeuId());
      canal.broadcast(renuncia);
      servidor.pararServicos();
    } else {
      MensagemCluster saindo = new MensagemCluster(TipoMensagemCluster.SAINDO, config.getMeuId());
      canal.broadcast(saindo);
    }
    canal.fechar();
    System.out.println("[CLUSTER] [INFO] Nó " + config.getMeuId() + " encerrado.");
  }
}
