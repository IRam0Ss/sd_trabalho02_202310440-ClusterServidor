/*****************************************************************
* Autor..............: Iury Ramos Sodre
* Matricula..........: 202310440
* Inicio.............: 15/06/2026
* Ultima alteracao...: 18/09/2026
* Nome...............: AtendimentoCliente
* Funcao.............: Thread dedicada para atendimento e processamento concorrente de requisicoes TCP de cada cliente.
*************************************************************** */

package controller;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.EOFException;
import java.net.Socket;
import java.util.List;
import Protocol.APDU;
import utils.InfoUser;
import utils.Protocolo;
import model.NotificadorClientes;

/**
 * Classe responsavel por atender clientes individuais via conexao TCP concorrente.
 * Cada conexao de cliente executa em uma thread independente, processando objetos {@link APDU}.
 * 
 * @author Iury Ramos Sodre (Matricula: 202310440)
 * @version 2.0
 * @since 15/06/2026
 */
public class AtendimentoCliente implements Runnable {

  private Socket conexao;
  private GerenciadorGrupos gerenciador;
  private InfoUser usuarioAssociado = null;
  private ObjectOutputStream saidaObjetos;

  private static final java.util.concurrent.atomic.AtomicLong totalPingsAtendidos = new java.util.concurrent.atomic.AtomicLong(0);
  private static volatile long ultimoLogPing = 0;

  public static long getTotalPingsAtendidos() {
    return totalPingsAtendidos.get();
  }

  /**
   * Construtor do AtendimentoCliente.
   * 
   * @param conexao     Socket TCP da conexao atual
   * @param gerenciador Referencia ao gerenciador de grupos e usuarios
   */
  public AtendimentoCliente(Socket conexao, GerenciadorGrupos gerenciador) {
    this.conexao = conexao;
    this.gerenciador = gerenciador;
    try {
      this.saidaObjetos = new ObjectOutputStream(conexao.getOutputStream());
      this.saidaObjetos.flush();
    } catch (Exception e) {
      System.err.println("[ATENDIMENTO] [ERROR] Falha ao criar escritor TCP: " + e.getMessage());
    }
  }

  /**
   * Laco principal de leitura de comandos APDU do cliente via stream de objetos TCP.
   * Realiza a leitura continua e despacha cada APDU para processamento interno.
   */
  @Override
  public void run() {
    try {
      ObjectInputStream tradutorObjetos = new ObjectInputStream(this.conexao.getInputStream());
      Protocol.APDU apduRecebida;

      // Le e trata as APDUs recebidas
      while (!conexao.isClosed()) {
        try {
          apduRecebida = (Protocol.APDU) tradutorObjetos.readObject();
          if (apduRecebida != null) {
            processarAPDU(apduRecebida);
          }
        } catch (EOFException e) {
          // Encerramento normal do stream pelo cliente
          break;
        }
      }
    } catch (EOFException e) {
      // Stream finalizado
    } catch (Exception e) {
      System.err.println("[ATENDIMENTO] [INFO] Conexao encerrada com o cliente: " + e.getMessage());
    } finally {
      // Nao removemos o usuario da lista global ao fechar o socket TCP, pois clientes transientes
      // abrem e fecham a conexao TCP a cada comando (ficando ativos no UDP).
      try {
        if (this.saidaObjetos != null) {
          this.saidaObjetos.close();
        }
        if (this.conexao != null && !this.conexao.isClosed()) {
          this.conexao.close();
        }
      } catch (Exception ignored) {
      }
    }
  }

  /**
   * Envia uma resposta textual serializada padronizada para o cliente via ObjectOutputStream.
   * Compativel com qualquer cliente que faca readObject de String.
   * 
   * @param operacao  Codigo de status (Protocolo.OK ou Protocolo.ERRO).
   * @param mensagem  Texto descritivo ou conteudo da listagem.
   */
  private void enviarResposta(String operacao, String mensagem) {
    try {
      String respostaStr;
      if (mensagem == null || mensagem.isEmpty()) {
        respostaStr = operacao + ": ";
      } else {
        respostaStr = operacao + ": " + mensagem;
      }
      this.saidaObjetos.writeUnshared(respostaStr);
      this.saidaObjetos.flush();
    } catch (Exception e) {
      System.err.println("[ATENDIMENTO] [ERROR] Erro ao enviar resposta: " + e.getMessage());
    }
  }

  /**
   * Processa a APDU recebida pelo servidor e designa o processamento para o
   * gerenciador.
   * 
   * @param apdu APDU recebida
   */
  private void processarAPDU(Protocol.APDU apdu) {
    String comando = apdu.getOperacao();

    if (!Protocolo.PING.equals(comando)) {
      System.out.println("[ATENDIMENTO] [INFO] Processando comando " + comando);
    }

    if (comando == null || comando.isEmpty()) {
      return;
    }

    switch (comando) {
      case Protocolo.PING:
        enviarResposta(Protocolo.OK, "ATIVO");
        long totalPings = totalPingsAtendidos.incrementAndGet();
        long agora = System.currentTimeMillis();
        if (agora - ultimoLogPing >= 15000) {
          ultimoLogPing = agora;
          System.out.println("[LIVENESS:PING] Heartbeats PING de clientes ativos sendo atendidos normalmente (Total: " + totalPings + ")");
        }
        break;

      case Protocolo.JOIN:
        String grupoJoin = apdu.getNomeGrupo();
        String ipJoin = conexao.getInetAddress().getHostAddress();
        InfoUser usuarioJoin = new InfoUser(apdu.getNomeUsuario(), ipJoin, apdu.getPortaClienteUDP());
        if (this.usuarioAssociado == null) {
          this.usuarioAssociado = usuarioJoin;
        }

        Operacao opJoin = Operacao.entrarGrupo(apdu.getIdRequisicao(), grupoJoin, apdu.getNomeUsuario(), ipJoin, apdu.getPortaClienteUDP());
        Resultado resJoin = this.gerenciador.aplicar(opJoin);
        enviarResposta(resJoin.getOperacaoRetorno(), resJoin.getMensagem());
        if (resJoin.isNotificarAtualizacaoUsuarios()) {
          NotificadorClientes.notificarAtualizacaoUsuarios(this.gerenciador.getTodosUsuariosAtivos());
        }
        System.out.println("[ATENDIMENTO] [INFO] Processamento de JOIN de '" + apdu.getNomeUsuario() + "' concluido.");
        break;

      case Protocolo.REGISTER:
        String ipReg = conexao.getInetAddress().getHostAddress();
        InfoUser usuarioRegister = new InfoUser(apdu.getNomeUsuario(), ipReg, apdu.getPortaClienteUDP());
        if (this.usuarioAssociado == null) {
          this.usuarioAssociado = usuarioRegister;
        }

        Operacao opReg = Operacao.registrarUsuario(apdu.getIdRequisicao(), apdu.getNomeUsuario(), ipReg, apdu.getPortaClienteUDP());
        Resultado resReg = this.gerenciador.aplicar(opReg);
        enviarResposta(resReg.getOperacaoRetorno(), resReg.getMensagem());
        if (resReg.isNotificarAtualizacaoUsuarios()) {
          NotificadorClientes.notificarAtualizacaoUsuarios(this.gerenciador.getTodosUsuariosAtivos());
        }
        System.out.println("[ATENDIMENTO] [INFO] Processamento de REGISTER de '" + apdu.getNomeUsuario() + "' concluido.");
        break;

      case Protocolo.LOGOUT:
        Operacao opLogout = Operacao.removerUsuario(apdu.getIdRequisicao(), apdu.getNomeUsuario());
        Resultado resLogout = this.gerenciador.aplicar(opLogout);
        enviarResposta(resLogout.getOperacaoRetorno(), resLogout.getMensagem());
        if (resLogout.isNotificarAtualizacaoUsuarios()) {
          NotificadorClientes.notificarAtualizacaoUsuarios(this.gerenciador.getTodosUsuariosAtivos());
        }
        System.out.println("[ATENDIMENTO] [INFO] Processamento de LOGOUT de '" + apdu.getNomeUsuario() + "' concluido.");
        break;

      case Protocolo.LEAVE:
        String grupoLeave = apdu.getNomeGrupo();
        if (grupoLeave != null && !grupoLeave.equalsIgnoreCase("GLOBAL")) {
          Operacao opLeave = Operacao.sairGrupo(apdu.getIdRequisicao(), grupoLeave, apdu.getNomeUsuario());
          Resultado resLeave = this.gerenciador.aplicar(opLeave);
          enviarResposta(resLeave.getOperacaoRetorno(), resLeave.getMensagem());
          System.out.println("[ATENDIMENTO] [INFO] Processamento de LEAVE de '" + apdu.getNomeUsuario() + "' concluido.");
        } else {
          Operacao opLeaveGlobal = Operacao.removerUsuario(apdu.getIdRequisicao(), apdu.getNomeUsuario());
          Resultado resLeaveGlobal = this.gerenciador.aplicar(opLeaveGlobal);
          enviarResposta(Protocolo.OK, "Desconectado do servidor");
          if (resLeaveGlobal.isNotificarAtualizacaoUsuarios()) {
            NotificadorClientes.notificarAtualizacaoUsuarios(this.gerenciador.getTodosUsuariosAtivos());
          }
        }
        break;

      case Protocolo.LIST:
        List<String> grupos = this.gerenciador.listarGrupos();
        if (grupos.isEmpty()) {
          enviarResposta(Protocolo.OK, "");
        } else {
          enviarResposta(Protocolo.OK, String.join(",", grupos));
        }
        System.out.println("[ATENDIMENTO] [INFO] Processamento de LIST concluido.");
        break;

      case Protocolo.USERS:
        java.util.Set<InfoUser> usuarios = this.gerenciador.getTodosUsuariosAtivos();
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (InfoUser u : usuarios) {
          if (!first)
            sb.append(",");
          sb.append(u.getNome());
          first = false;
        }
        enviarResposta(Protocolo.OK, sb.toString());
        System.out.println(
            "[ATENDIMENTO] [INFO] Processamento de USERS concluido. Total: " + usuarios.size());
        break;

      case Protocolo.MEMBERS:
        String grupoList = apdu.getNomeGrupo();
        List<InfoUser> membrosGrupo = this.gerenciador.getTodosMembros(grupoList);
        StringBuilder sbMembers = new StringBuilder();
        boolean firstMember = true;
        for (InfoUser u : membrosGrupo) {
          if (!firstMember)
            sbMembers.append(",");
          sbMembers.append(u.getNome());
          firstMember = false;
        }
        enviarResposta(Protocolo.OK, sbMembers.toString());
        System.out.println(
            "[ATENDIMENTO] [INFO] Processamento de MEMBERS concluido para grupo: " + grupoList);
        break;

      case Protocolo.BLOCK:
        String usuarioABloquear = apdu.getDestinatario();
        String bloqueador = apdu.getNomeUsuario();
        Operacao opBlock = Operacao.bloquear(apdu.getIdRequisicao(), bloqueador, usuarioABloquear);
        Resultado resBlock = this.gerenciador.aplicar(opBlock);
        enviarResposta(resBlock.getOperacaoRetorno(), resBlock.getMensagem());
        System.out.println("[ATENDIMENTO] [INFO] Usuario '" + bloqueador + "' bloqueou '" + usuarioABloquear + "'");
        break;

      case Protocolo.UNBLOCK:
        String usuarioADesbloquear = apdu.getDestinatario();
        String desbloqueador = apdu.getNomeUsuario();
        Operacao opUnblock = Operacao.desbloquear(apdu.getIdRequisicao(), desbloqueador, usuarioADesbloquear);
        Resultado resUnblock = this.gerenciador.aplicar(opUnblock);
        enviarResposta(resUnblock.getOperacaoRetorno(), resUnblock.getMensagem());
        System.out.println("[ATENDIMENTO] [INFO] Usuario '" + desbloqueador + "' desbloqueou '" + usuarioADesbloquear + "'");
        break;

      default:
        System.err.println("[ATENDIMENTO] [ERROR] Comando desconhecido: " + comando);
        enviarResposta(Protocolo.ERRO, "Comando desconhecido: " + comando);
        break;
    }
  }
}
