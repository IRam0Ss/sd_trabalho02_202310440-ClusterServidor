/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 15/10/2026
* Nome...............: Principal
* Funcao.............: Ponto de entrada do Servidor com suporte a cluster dinamico, auto-atribuicao de ID e console interativo.
*************************************************************** */

import java.util.Scanner;

import cluster.BeaconClusterLAN;
import cluster.ConfigCluster;
import cluster.NoCluster;
import cluster.VisaoCluster;
import controller.AtendimentoCliente;
import controller.GerenciadorGrupos;

/**
 * Ponto de entrada do servidor E.D.E.N. com suporte a redundancia (N servidores).
 * Oferece atribuicao automatica de ID (sem necessidade de digitacao manual),
 * suporte a cluster aberto e infinito, e comandos interativos de depuracao:
 * status, estado, ajuda e sair.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 4.0
 */
public class Principal {

  public static void main(String[] args) {
    System.setProperty("java.net.preferIPv4Stack", "true");

    ConfigCluster config = null;
    BeaconClusterLAN beaconLAN = null;
    Scanner scanner = new Scanner(System.in);

    try {
      if (args.length >= 1) {
        String flag = args[0].toLowerCase();

        if ("--local".equals(flag)) {
          Integer idForcado = (args.length >= 2) ? Integer.parseInt(args[1]) : null;
          if (args.length >= 3) {
            // Suporte legado a scripts de teste: --local <id> <total>
            int id = Integer.parseInt(args[1]);
            int total = Integer.parseInt(args[2]);
            config = ConfigCluster.gerarConfigLocal(id, total);
          } else {
            config = ConfigCluster.descobrirEConfigurarLocal(idForcado);
          }

        } else if ("--rede".equals(flag) || "--lan".equals(flag)) {
          Integer idForcado = (args.length >= 2) ? Integer.parseInt(args[1]) : null;
          config = ConfigCluster.descobrirEConfigurarRede(idForcado);

        } else {
          // Argumento numerico legado ou arquivo properties: <id> [arquivo]
          try {
            int id = Integer.parseInt(args[0]);
            String arquivo = (args.length >= 2) ? args[1] : "cluster.properties";
            config = ConfigCluster.carregarArquivo(arquivo, id);
          } catch (NumberFormatException nfe) {
            config = ConfigCluster.descobrirEConfigurarLocal(null);
          }
        }

      } else {
        // Modo interativo amigavel sem solicitar IDs ou quantidades manuais
        System.out.println("==================================================");
        System.out.println("        E.D.E.N. - SERVIDOR DISTRIBUIDO           ");
        System.out.println("==================================================");
        System.out.println("Selecione o modo de execucao:");
        System.out.println("  [1] LOCAL - Mesma maquina (atribuicao automatica de portas e ID)");
        System.out.println("  [2] REDE  - Maquinas distintas na mesma rede local (LAN)");
        System.out.print("Escolha [1/2] (Pressione ENTER para LOCAL): ");

        String linha = scanner.hasNextLine() ? scanner.nextLine().trim() : "";
        if ("2".equals(linha) || "rede".equalsIgnoreCase(linha) || "lan".equalsIgnoreCase(linha)) {
          config = ConfigCluster.descobrirEConfigurarRede(null);
        } else {
          config = ConfigCluster.descobrirEConfigurarLocal(null);
        }
      }

    } catch (Exception e) {
      System.err.println("[INICIO] [AVISO] Falha ao processar argumentos (" + e.getMessage() + "). Assumindo auto-configuracao local.");
      config = ConfigCluster.descobrirEConfigurarLocal(null);
    }

    GerenciadorGrupos gerenciador = new GerenciadorGrupos();
    NoCluster noCluster = new NoCluster(config, gerenciador);

    // Se estiver rodando em modo rede, ativa o beacon de broadcast UDP para novos nós
    if (!"127.0.0.1".equals(config.getMeuNoInfo().host)) {
      beaconLAN = new BeaconClusterLAN(config, novoPar -> {
        noCluster.getVisao().adicionarParSeAusente(novoPar);
        noCluster.getCanal().adicionarParSeAusente(novoPar.id, novoPar.host, novoPar.portaCluster);
      });
      beaconLAN.iniciar();
    }

    final BeaconClusterLAN finalBeacon = beaconLAN;

    // Registra shutdown hook para saida graciosa e failover imediato
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("\n[SISTEMA] Sinal de encerramento detectado. Finalizando nó...");
      if (finalBeacon != null) {
        finalBeacon.parar();
      }
      noCluster.encerrar();
    }));

    // Inicia o cluster
    noCluster.iniciar();

    System.out.println("\n[SISTEMA] Servidor iniciado com sucesso. Digite 'ajuda' para comandos de depuracao.\n");

    // Loop interativo de comandos de console para demonstracao e debug (D5)
    while (true) {
      try {
        if (!scanner.hasNextLine()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            break;
          }
          continue;
        }
        String comando = scanner.nextLine().trim().toLowerCase();
        if (comando.isEmpty()) continue;

        switch (comando) {
          case "status":
            exibirStatus(noCluster, config);
            break;

          case "estado":
            gerenciador.imprimirEstado();
            break;

          case "ajuda":
          case "help":
            exibirAjuda();
            break;

          case "sair":
          case "exit":
          case "quit":
            System.out.println("[SISTEMA] Encerrando o servidor...");
            if (finalBeacon != null) finalBeacon.parar();
            noCluster.encerrar();
            System.exit(0);
            break;

          default:
            System.out.println("Comando desconhecido: '" + comando + "'. Digite 'ajuda' para a lista de comandos.");
            break;
        }

      } catch (Exception e) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          break;
        }
      }
    }
  }

  private static void exibirStatus(NoCluster noCluster, ConfigCluster config) {
    System.out.println("\n=== STATUS DO CLUSTER [Nó " + noCluster.getMeuId() + "] ===");
    System.out.println("Papel Local: " + noCluster.getPapel() + " | Epoca: " + noCluster.getEpoca());
    System.out.println("Lider Ativo Conhecido: Nó " + (noCluster.getIdLiderAtivo() > 0 ? noCluster.getIdLiderAtivo() : "NENHUM"));
    ConfigCluster.NoInfo meuInfo = config.getMeuNoInfo();
    System.out.printf("Portas Locais: Cluster=%d | TCP=%d | UDP=%d\n", meuInfo.portaCluster, meuInfo.portaTcp, meuInfo.portaUdp);
    System.out.println("Total de Nós Conhecidos na Topologia: " + config.getNos().size());
    System.out.println("Heartbeats PING Atendidos (Liveness Clientes): " + AtendimentoCliente.getTotalPingsAtendidos());

    VisaoCluster visao = noCluster.getVisao();
    System.out.println("Status dos Pares Conhecidos:");
    for (VisaoCluster.ParEstado p : visao.getTodosPares()) {
      long deltaHb = (p.ultimoHeartbeat > 0) ? (System.currentTimeMillis() - p.ultimoHeartbeat) : -1;
      System.out.printf("  [Nó %d] -> %s:%d | Estado: %s | Epoca: %d | Versao: %d | Conectado: %b | Ultimo HB: %s\n",
          p.id, p.host, p.portaCluster, p.estado, p.epoca, p.versao, p.conectado,
          deltaHb >= 0 ? (deltaHb + "ms atras") : "nunca");
    }
    System.out.println("====================================================\n");
  }

  private static void exibirAjuda() {
    System.out.println("\nComandos disponiveis no console:");
    System.out.println("  status   - Exibe estado local, conexoes do cluster, total de nós e lider ativo.");
    System.out.println("  estado   - Exibe dump da estrutura replicada (usuarios, salas, bloqueios e hash).");
    System.out.println("  ajuda    - Mostra esta mensagem de ajuda.");
    System.out.println("  sair     - Encerra este nó graciosamente com failover imediato para os outros.\n");
  }

}
