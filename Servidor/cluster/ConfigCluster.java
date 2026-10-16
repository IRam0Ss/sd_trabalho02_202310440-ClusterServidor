/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/10/2026
* Ultima alteracao...: 15/10/2026
* Nome...............: ConfigCluster
* Funcao.............: Gerenciador de topologia dinamica do cluster com suporte a atribuicao automatica de ID e crescimento infinito.
*************************************************************** */

package cluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import utils.Protocolo;

/**
 * Carrega e disponibiliza os metadados de conectividade de cada nó do cluster.
 * Suporta atribuicao automatica de IDs (sem intervencao manual do usuario),
 * cluster aberto e infinito que aceita novos membros a qualquer instante,
 * execucao local e distribuicao em rede LAN via auto-descoberta.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 2.0
 */
public class ConfigCluster implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Metadados de rede de um nó do cluster.
   */
  public static class NoInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    public final int id;
    public final String host;
    public final int portaCluster;
    public final int portaTcp;
    public final int portaUdp;
    public final int portaDiscovery;

    public NoInfo(int id, String host, int portaCluster, int portaTcp, int portaUdp, int portaDiscovery) {
      this.id = id;
      this.host = host;
      this.portaCluster = portaCluster;
      this.portaTcp = portaTcp;
      this.portaUdp = portaUdp;
      this.portaDiscovery = portaDiscovery;
    }

    @Override
    public String toString() {
      return String.format("Nó[%d: host=%s, cluster=%d, tcp=%d, udp=%d]", id, host, portaCluster, portaTcp, portaUdp);
    }
  }

  private final int meuId;
  private final Map<Integer, NoInfo> nos;

  public ConfigCluster(int meuId, Map<Integer, NoInfo> nos) {
    this.meuId = meuId;
    this.nos = new ConcurrentHashMap<>(nos);
  }

  public int getMeuId() {
    return meuId;
  }

  public NoInfo getMeuNoInfo() {
    return nos.get(meuId);
  }

  public Map<Integer, NoInfo> getNos() {
    return Collections.unmodifiableMap(nos);
  }

  public List<NoInfo> getOutrosNos() {
    List<NoInfo> outros = new ArrayList<>();
    for (NoInfo n : nos.values()) {
      if (n.id != meuId) {
        outros.add(n);
      }
    }
    return outros;
  }

  /**
   * Registra um novo nó descoberto dinamicamente durante a execucao do cluster.
   */
  public void registrarParDinamico(NoInfo info) {
    if (info != null && !nos.containsKey(info.id)) {
      nos.put(info.id, info);
      System.out.println("[CONFIG] Novo nó integrado a topologia: Nó " + info.id + " (" + info.host + ":" + info.portaCluster + ")");
    }
  }

  /**
   * Testa se uma porta TCP local esta disponivel para bind.
   */
  public static boolean isPortaDisponivel(String host, int porta) {
    try (ServerSocket ss = new ServerSocket()) {
      ss.setReuseAddress(true);
      ss.bind(new InetSocketAddress(host, porta));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Testa se ha algum servico respondendo em uma porta TCP.
   */
  public static boolean isPortaEmUso(String host, int porta) {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress(host, porta), 100);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Atribui automaticamente o ID local e descobre os outros nós locais ativos
   * na mesma maquina escaneando os offsets de porta.
   * 
   * @param idForcado ID opcional forcado por linha de comando (se null, detecta automaticamente).
   * @return Objeto ConfigCluster pronto para uso.
   */
  public static ConfigCluster descobrirEConfigurarLocal(Integer idForcado) {
    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>();
    int idEscolhido = -1;

    if (idForcado != null && idForcado > 0) {
      idEscolhido = idForcado;
    } else {
      // Escaneia os IDs de 1 a 100 para encontrar o primeiro disponivel
      for (int k = 1; k <= 100; k++) {
        int portaCluster = 9000 + k;
        if (!isPortaEmUso("127.0.0.1", portaCluster)) {
          idEscolhido = k;
          break;
        }
      }
      if (idEscolhido <= 0) {
        idEscolhido = 1;
      }
    }

    // Registra o proprio nó
    int minhaPortaCluster = 9000 + idEscolhido;
    int minhaPortaTcp = 6789 + 10 * (idEscolhido - 1);
    int minhaPortaUdp = 7777 + 10 * (idEscolhido - 1);
    int minhaPortaDisc = Protocolo.PORTA_DISCOVERY;
    NoInfo meuInfo = new NoInfo(idEscolhido, "127.0.0.1", minhaPortaCluster, minhaPortaTcp, minhaPortaUdp, minhaPortaDisc);
    mapa.put(idEscolhido, meuInfo);

    // Detecta outros nós locais que ja estejam em execucao
    for (int j = 1; j <= 50; j++) {
      if (j == idEscolhido) continue;
      int pCluster = 9000 + j;
      if (isPortaEmUso("127.0.0.1", pCluster)) {
        int pTcp = 6789 + 10 * (j - 1);
        int pUdp = 7777 + 10 * (j - 1);
        mapa.put(j, new NoInfo(j, "127.0.0.1", pCluster, pTcp, pUdp, Protocolo.PORTA_DISCOVERY));
      }
    }

    System.out.printf("[CONFIG:AUTO] ID atribuido automaticamente: Nó %d | Portas: Cluster=%d, TCP=%d, UDP=%d\n",
        idEscolhido, minhaPortaCluster, minhaPortaTcp, minhaPortaUdp);
    if (mapa.size() > 1) {
      System.out.print("[CONFIG:AUTO] Nós locais detectados previamente em execucao: ");
      for (Integer id : mapa.keySet()) {
        if (id != idEscolhido) System.out.print("Nó " + id + " ");
      }
      System.out.println();
    } else {
      System.out.println("[CONFIG:AUTO] Nenhum outro nó local ativo encontrado. Este nó sera o pioneiro do cluster.");
    }

    return new ConfigCluster(idEscolhido, mapa);
  }

  /**
   * Atribui automaticamente o ID e descobre os pares via broadcast UDP na rede local (LAN).
   * 
   * @param idForcado ID opcional forcado por linha de comando (se null, detecta automaticamente).
   * @return Objeto ConfigCluster pronto para uso em rede.
   */
  public static ConfigCluster descobrirEConfigurarRede(Integer idForcado) {
    String meuIp = BeaconClusterLAN.obterIpLocalLAN();
    int portaClusterPadrao = 9000;
    int portaTcpPadrao = Protocolo.PORTA_SERVIDOR_TCP;
    int portaUdpPadrao = Protocolo.PORTA_SERVIDOR_UDP;

    System.out.println("[CONFIG:REDE] Sondando presenca de cluster existente na rede local via broadcast...");
    Map<Integer, NoInfo> paresRede = BeaconClusterLAN.sondarParesNaRede(meuIp, portaClusterPadrao, portaTcpPadrao, portaUdpPadrao);

    int idEscolhido = 1;
    if (idForcado != null && idForcado > 0) {
      idEscolhido = idForcado;
    } else if (!paresRede.isEmpty()) {
      int maior = 0;
      for (Integer id : paresRede.keySet()) {
        if (id > maior) maior = id;
      }
      idEscolhido = maior + 1;
    }

    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>(paresRede);
    NoInfo meuInfo = new NoInfo(idEscolhido, meuIp, portaClusterPadrao, portaTcpPadrao, portaUdpPadrao, Protocolo.PORTA_DISCOVERY);
    mapa.put(idEscolhido, meuInfo);

    System.out.printf("[CONFIG:REDE] ID atribuido automaticamente: Nó %d (%s) | Pares encontrados: %d\n",
        idEscolhido, meuIp, paresRede.size());

    return new ConfigCluster(idEscolhido, mapa);
  }

  /**
   * Metodo legado para compatibilidade com testes que informam (id, totalNos).
   */
  public static ConfigCluster gerarConfigLocal(int meuId, int totalNos) {
    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>();
    for (int k = 1; k <= totalNos; k++) {
      int portaCluster = 9000 + k;
      int portaTcp = 6789 + 10 * (k - 1);
      int portaUdp = 7777 + 10 * (k - 1);
      int portaDisc = Protocolo.PORTA_DISCOVERY;
      mapa.put(k, new NoInfo(k, "127.0.0.1", portaCluster, portaTcp, portaUdp, portaDisc));
    }
    return new ConfigCluster(meuId, mapa);
  }

  /**
   * Carrega configuracao a partir de um arquivo .properties para casos especificos.
   */
  public static ConfigCluster carregarArquivo(String caminho, int meuId) throws Exception {
    File f = new File(caminho);
    if (!f.exists()) {
      throw new IllegalArgumentException("Arquivo de configuracao do cluster nao encontrado: " + caminho);
    }

    Map<Integer, NoInfo> mapa = new ConcurrentHashMap<>();

    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"))) {
      String linha;
      while ((linha = br.readLine()) != null) {
        linha = linha.trim();
        if (linha.isEmpty() || linha.startsWith("#")) {
          continue;
        }

        String[] partesIgual = linha.split("=", 2);
        if (partesIgual.length < 2) continue;

        String chave = partesIgual[0].trim();
        if (!chave.startsWith("no.")) continue;

        int id = Integer.parseInt(chave.substring(3).trim());
        String resto = partesIgual[1].trim();

        String[] tokens = resto.split(";");
        String host = tokens[0].trim();
        int portaCluster = 9000 + id;
        int portaTcp = Protocolo.PORTA_SERVIDOR_TCP;
        int portaUdp = Protocolo.PORTA_SERVIDOR_UDP;
        int portaDisc = Protocolo.PORTA_DISCOVERY;

        for (int i = 1; i < tokens.length; i++) {
          String tok = tokens[i].trim();
          if (tok.startsWith("cluster=")) {
            portaCluster = Integer.parseInt(tok.substring(8).trim());
          } else if (tok.startsWith("tcp=")) {
            portaTcp = Integer.parseInt(tok.substring(4).trim());
          } else if (tok.startsWith("udp=")) {
            portaUdp = Integer.parseInt(tok.substring(4).trim());
          } else if (tok.startsWith("discovery=")) {
            portaDisc = Integer.parseInt(tok.substring(10).trim());
          }
        }

        mapa.put(id, new NoInfo(id, host, portaCluster, portaTcp, portaUdp, portaDisc));
      }
    }

    if (!mapa.containsKey(meuId)) {
      throw new IllegalArgumentException("O ID informado (" + meuId + ") nao consta no arquivo de configuracao.");
    }

    return new ConfigCluster(meuId, mapa);
  }
}
