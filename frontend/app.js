const eventoEl = document.querySelector("#evento");
const infoEl = document.querySelector("#evento-info");
const mapaEl = document.querySelector("#mapa");
const formEl = document.querySelector("#form");
const statusEl = document.querySelector("#status");
const rajadaEl = document.querySelector("#rajada");

const FINAIS = new Set(["CONFIRMADA", "ASSENTO_INDISPONIVEL", "PAGAMENTO_RECUSADO"]);
const ROTULOS = {
  EM_PROCESSAMENTO: "Em processamento",
  AGUARDANDO_PAGAMENTO: "Aguardando pagamento",
  PAGO: "Pago",
  CONFIRMADA: "Confirmada",
  ASSENTO_INDISPONIVEL: "Assento indisponível",
  PAGAMENTO_RECUSADO: "Pagamento recusado",
};

let assentoSelecionado = null;
let pollTimer = null;
let mapaTimer = null;

function reais(centavos) {
  return (centavos / 100).toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function mostrarStatus(texto, erro) {
  statusEl.textContent = texto;
  statusEl.classList.toggle("erro", Boolean(erro));
}

async function carregarEventos() {
  const eventos = await fetch("/api/eventos").then((r) => r.json());
  eventoEl.innerHTML = "";
  for (const evento of eventos) {
    const opcao = document.createElement("option");
    opcao.value = evento.id;
    opcao.textContent = evento.nome;
    opcao.dataset.local = evento.local;
    opcao.dataset.data = evento.data_hora;
    opcao.dataset.preco = evento.preco_centavos;
    eventoEl.appendChild(opcao);
  }
  await carregarAssentos();
}

async function carregarAssentos() {
  const id = eventoEl.value;
  if (!id) return;
  const opcao = eventoEl.selectedOptions[0];
  infoEl.textContent = `${opcao.dataset.local} · ${opcao.dataset.data} · ${reais(Number(opcao.dataset.preco))}`;

  const resposta = await fetch(`/api/eventos/${id}/assentos`);
  const assentos = await resposta.json();
  const selecionadoAindaLivre = assentos.some(
    (a) => a.numero === assentoSelecionado && a.status === "LIVRE"
  );
  if (!selecionadoAindaLivre) assentoSelecionado = null;

  const botoes = [...mapaEl.querySelectorAll("button")];
  const mesmoMapa =
    botoes.length === assentos.length &&
    assentos.every((assento, i) => Number(botoes[i].textContent) === assento.numero);

  function pintar(botao, assento) {
    botao.className = `assento ${assento.status.toLowerCase()}`;
    botao.disabled = assento.status !== "LIVRE";
    if (assento.status === "LIVRE" && assento.numero === assentoSelecionado) {
      botao.classList.add("selecionado");
    }
  }

  // Atualiza no lugar quando o mapa já existe. Recriar os botões a cada
  // refresh derrubaria o clique no meio da escolha do assento.
  if (mesmoMapa) {
    assentos.forEach((assento, i) => pintar(botoes[i], assento));
    return;
  }

  mapaEl.innerHTML = "";
  for (const assento of assentos) {
    const botao = document.createElement("button");
    botao.type = "button";
    botao.textContent = assento.numero;
    pintar(botao, assento);
    botao.addEventListener("click", () => {
      assentoSelecionado = assento.numero;
      carregarAssentos();
    });
    mapaEl.appendChild(botao);
  }
}

function pararPoll() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

function acompanhar(reservaId, prefixo) {
  pararPoll();
  mostrarStatus(`${prefixo}\nReserva recebida. Em processamento.\n${reservaId}`);

  async function atualizar() {
    const resposta = await fetch(`/api/reservas/${reservaId}`);
    if (!resposta.ok) {
      pararPoll();
      mostrarStatus("Não foi possível consultar a reserva.", true);
      return;
    }
    const reserva = await resposta.json();
    const rotulo = ROTULOS[reserva.status] || reserva.status;
    mostrarStatus(`${prefixo}\n${rotulo}\nAssento ${reserva.numero} · ${reservaId}`);
    if (reserva.final || FINAIS.has(reserva.status)) pararPoll();
    carregarAssentos();
  }

  pollTimer = setInterval(atualizar, 1000);
}

formEl.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!assentoSelecionado) {
    mostrarStatus("Escolha um assento livre.", true);
    return;
  }
  const enviar = document.querySelector("#enviar");
  enviar.disabled = true;
  try {
    const corpo = {
      evento_id: Number(eventoEl.value),
      numero: assentoSelecionado,
      nome: document.querySelector("#nome").value,
      email: document.querySelector("#email").value,
      cartao: document.querySelector("#cartao").value,
    };
    const resposta = await fetch("/api/reservas", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(corpo),
    });
    const dados = await resposta.json();
    if (!resposta.ok) {
      mostrarStatus(dados.erro || "Não foi possível reservar.", true);
      return;
    }
    acompanhar(dados.reserva_id, `Assento ${corpo.numero}`);
    assentoSelecionado = null;
  } catch (err) {
    mostrarStatus("Falha de rede ao enviar a reserva.", true);
  } finally {
    enviar.disabled = false;
  }
});

rajadaEl.addEventListener("click", async () => {
  const id = eventoEl.value;
  rajadaEl.disabled = true;
  try {
    const assentos = await fetch(`/api/eventos/${id}/assentos`).then((r) => r.json());
    const livres = assentos.filter((a) => a.status === "LIVRE").slice(0, 10);
    if (livres.length < 10) {
      mostrarStatus(`Só há ${livres.length} assento(s) livre(s). A rajada precisa de 10.`, true);
      return;
    }
    mostrarStatus("Enviando 10 reservas…");
    const envios = livres.map((assento, i) =>
      fetch("/api/reservas", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          evento_id: Number(id),
          numero: assento.numero,
          nome: `Rajada ${i + 1}`,
          email: `rajada${i + 1}@exemplo.com`,
          cartao: "4111111111111111",
        }),
      }).then(async (resposta) => ({ ok: resposta.ok, status: resposta.status, corpo: await resposta.json() }))
    );
    const resultados = await Promise.all(envios);
    const ok = resultados.filter((r) => r.ok).length;
    const erros = [...new Set(resultados.filter((r) => !r.ok).map((r) => r.corpo.erro || String(r.status)))];
    mostrarStatus(
      `Rajada: ${ok} de 10 aceitas (202).\n` + (erros.length ? erros.join("\n") : "As mensagens seguem para os workers.")
    );
    carregarAssentos();
  } catch (err) {
    mostrarStatus("Falha de rede na rajada.", true);
  } finally {
    rajadaEl.disabled = false;
  }
});

// --------------------------------------------------------------------------
// Testes de assento e pagamento (Fases 4 e 5)
// --------------------------------------------------------------------------
// O status da reserva só muda quando o status_service existir. Por isso estes
// testes acompanham a cor do assento no banco, que é o que as fases 4 e 5 alteram.
const TESTES = {
  aprovado: {
    cartao: "4111111111111111",
    titulo: "Teste: cartão aprovado",
    terminou: (h) => h.at(-1) === "VENDIDO",
    explicacao: "assento.reservado -> pagamento.aprovado -> assento VENDIDO.",
  },
  recusado: {
    cartao: "4111111111110000",
    titulo: "Teste: cartão recusado (sem limite)",
    terminou: (h) => h.includes("BLOQUEADO") && h.at(-1) === "LIVRE",
    explicacao: "pagamento.recusado -> compensação: o assento voltou a ficar LIVRE para outra pessoa.",
  },
  fraude: {
    cartao: "9999111111111111",
    titulo: "Teste: fraude",
    // Fraude não gera evento nenhum: o assento fica BLOQUEADO. Espera passar o
    // tempo do pagamento (~2 s) para ter certeza de que não foi vendido.
    terminou: (h, segundos) => h.at(-1) === "BLOQUEADO" && segundos >= 6,
    explicacao: "payment_service deu NACK: a mensagem está na q.dlq e o assento fica BLOQUEADO.",
  },
  disputa: {
    cartao: "4111111111111111",
    titulo: "Teste: 2 pessoas no mesmo assento",
    terminou: (h) => h.at(-1) === "VENDIDO",
    explicacao: "Só uma reserva ganhou o assento. A outra recebeu assento.indisponivel (veja o log do assentos_service).",
  },
};
const LIMITE_SEGUNDOS = 20;
const botoesTeste = [...document.querySelectorAll("[data-teste]")];

function enviarReserva(numero, nome, email, cartao) {
  return fetch("/api/reservas", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ evento_id: Number(eventoEl.value), numero, nome, email, cartao }),
  }).then(async (resposta) => ({ ok: resposta.ok, corpo: await resposta.json() }));
}

// Guarda cada mudança de status do assento (LIVRE -> BLOQUEADO -> ...) até o teste terminar.
function acompanharAssento(numero, teste) {
  pararPoll();
  const historico = ["LIVRE"];
  const inicio = Date.now();

  async function atualizar() {
    const assentos = await fetch(`/api/eventos/${eventoEl.value}/assentos`).then((r) => r.json());
    const atual = assentos.find((a) => a.numero === numero).status;
    if (atual !== historico.at(-1)) historico.push(atual);
    const segundos = (Date.now() - inicio) / 1000;
    const linhas = [teste.titulo, `Assento ${numero}: ${historico.join(" -> ")}`];

    if (teste.terminou(historico, segundos)) {
      pararPoll();
      linhas.push(`OK. ${teste.explicacao}`);
    } else if (segundos > LIMITE_SEGUNDOS) {
      pararPoll();
      linhas.push("Tempo esgotado. Confira se o assentos_service e o payment_service estão ligados.");
    } else {
      linhas.push("Aguardando os workers…");
    }
    mostrarStatus(linhas.join("\n"), segundos > LIMITE_SEGUNDOS);
    carregarAssentos();
  }

  pollTimer = setInterval(atualizar, 1000);
}

async function rodarTeste(tipo) {
  const teste = TESTES[tipo];
  const assentos = await fetch(`/api/eventos/${eventoEl.value}/assentos`).then((r) => r.json());
  const livre = (n) => assentos.some((a) => a.numero === n && a.status === "LIVRE");
  const numero = livre(assentoSelecionado) ? assentoSelecionado : assentos.find((a) => a.status === "LIVRE")?.numero;
  if (!numero) {
    mostrarStatus("Não há assento livre neste evento.", true);
    return;
  }
  const nome = document.querySelector("#nome").value || "Teste";
  const email = document.querySelector("#email").value || "teste@exemplo.com";

  // Disputa: as duas reservas saem juntas (Promise.all) para o mesmo assento.
  const pessoas = tipo === "disputa" ? ["Pessoa A", "Pessoa B"] : [nome];
  const respostas = await Promise.all(
    pessoas.map((p, i) => enviarReserva(numero, p, tipo === "disputa" ? `pessoa${i + 1}@exemplo.com` : email, teste.cartao))
  );
  const falha = respostas.find((r) => !r.ok);
  if (falha) {
    mostrarStatus(falha.corpo.erro || "Não foi possível reservar.", true);
    return;
  }
  assentoSelecionado = null;
  acompanharAssento(numero, teste);
}

for (const botao of botoesTeste) {
  botao.addEventListener("click", async () => {
    botoesTeste.forEach((b) => (b.disabled = true));
    try {
      await rodarTeste(botao.dataset.teste);
    } catch (err) {
      mostrarStatus("Falha de rede no teste.", true);
    } finally {
      botoesTeste.forEach((b) => (b.disabled = false));
    }
  });
}

eventoEl.addEventListener("change", () => {
  assentoSelecionado = null;
  carregarAssentos();
});

carregarEventos().catch(() => mostrarStatus("Não foi possível carregar os eventos.", true));
mapaTimer = setInterval(() => {
  if (eventoEl.value) carregarAssentos();
}, 2000);
