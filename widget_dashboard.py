# -*- coding: utf-8 -*-
"""
Módulo: widget_dashboard.py
=============================
Painel executivo (aba Dashboard) da JanelaGestaoDados.

Contém três classes interdependentes que compõem a visualização:

  WidgetDashboard   : orquestra cards de KPIs, gauges semicirculares e
                      gráfico de cobertura por sistema hídrico. Todas as
                      queries SQL estão declaradas como atributos de classe
                      (SQL_*) para facilitar manutenção e testes isolados.

  GaugeSemicircular : widget QPainter de velocímetro semicircular (180°)
                      com escala de cores automática:
                        verde  (#1cc88a) ≥ 70 %
                        amarelo(#f6c23e) ≥ 40 %
                        vermelho(#e74a3b) < 40 %

  GraficoSisHidrico : widget QPainter de barras horizontais por sistema
                      hídrico, com altura dinâmica e compatível com
                      QScrollArea.

Autor : SFI/ANA
Versão: 2.0 – Março/2026
"""

# ---------------------------------------------------------------------------
# Imports Qt – widgets, layout, renderização e utilitários de UI
# ---------------------------------------------------------------------------
from qgis.PyQt.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QFrame, QScrollArea, QSizePolicy, QMessageBox,
    QApplication,
)
from qgis.PyQt.QtCore import Qt, QRectF
from qgis.PyQt.QtGui import (
    QColor, QPainter, QFont, QPen,
)



class GaugeSemicircular(QWidget):
    """Widget de velocímetro semicircular para exibição de percentual de cobertura.

    Renderiza inteiramente via QPainter (sem imagens externas), desenhando
    dois arcos concêntricos sobrepostos sobre uma área semicircular de 180°:
    um arco de fundo cinza representando o total (100 %) e um arco colorido
    que avança proporcionalmente ao percentual calculado. No centro do
    semicírculo é exibido o valor percentual em negrito; abaixo do arco,
    um rótulo categórico identifica o indicador.

    A cor do arco de progresso é determinada automaticamente em três faixas:
        - **Verde** (``#1cc88a``): percentual ≥ 70 %
        - **Amarelo** (``#f6c23e``): percentual entre 40 % e 69 %
        - **Vermelho** (``#e74a3b``): percentual < 40 %

    O widget é não-interativo e atualiza sua renderização via ``update()``
    sempre que ``set_valor`` for chamado.

    Attributes:
        titulo (str): Rótulo exibido abaixo do arco, em maiúsculas.
        cor (QColor): Cor base do arco (sobrescrita pela lógica de faixas
            em ``_cor_por_percentual``).
        percentual (float): Valor atual entre 0,0 e 100,0 utilizado
            para determinar a extensão angular do arco.
    """
    
    def __init__(self, titulo, cor="#4e73df", parent=None):
        super().__init__(parent)
        self.titulo   = titulo
        self.cor      = QColor(cor)
        self.percentual = 0.0
        self.setMinimumSize(220, 150)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

    def set_valor(self, valor, total):
        """Atualiza o percentual e redesenha."""
        self.percentual = (valor / total * 100) if total > 0 else 0.0
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        w, h = self.width(), self.height()

        # Área útil do semicírculo
        margem   = 16
        diametro = min(w - margem * 2, (h - margem) * 2)
        raio     = diametro / 2
        cx       = w / 2
        cy       = h - margem - 24   # reserva espaço para texto abaixo

        ret = QRectF(cx - raio, cy - raio, diametro, diametro)

        espessura = max(14, int(raio * 0.18))

        # --- Arco de fundo (cinza) ---
        pen_bg = QPen(QColor("#e0e0e0"), espessura, Qt.SolidLine, Qt.FlatCap)
        painter.setPen(pen_bg)
        # Qt: 0° = direita, sentido anti-horário; usamos 180°→0° (semicírculo superior)
        painter.drawArc(ret, 180 * 16, -180 * 16)

        # --- Arco de progresso ---
        angulo_span = int(self.percentual / 100 * 180 * 16)
        cor_arco    = self._cor_por_percentual()
        pen_fg = QPen(cor_arco, espessura, Qt.SolidLine, Qt.FlatCap)
        painter.setPen(pen_fg)
        painter.drawArc(ret, 180 * 16, -angulo_span)

        # --- Texto percentual no centro ---
        font_pct = QFont()
        font_pct.setPointSize(14)
        font_pct.setBold(True)
        painter.setFont(font_pct)
        painter.setPen(QColor("#2c2c2c"))
        txt_pct = f"{self.percentual:.1f}%"
        painter.drawText(
            QRectF(cx - raio, cy - raio * 0.45, diametro, raio * 0.6),
            Qt.AlignCenter, txt_pct
        )

        # --- Rótulo abaixo ---
        font_lbl = QFont()
        font_lbl.setPointSize(8)
        painter.setFont(font_lbl)
        painter.setPen(QColor("#888888"))
        painter.drawText(
            QRectF(margem, cy + 4, w - margem * 2, 20),
            Qt.AlignCenter, self.titulo.upper()
        )

        painter.end()

    def _cor_por_percentual(self):
        """Retorna cor verde, amarela ou vermelha conforme o percentual."""
        if self.percentual >= 70:
            return QColor("#1cc88a")   # verde
        elif self.percentual >= 40:
            return QColor("#f6c23e")   # amarelo
        else:
            return QColor("#e74a3b")   # vermelho
  


class GraficoUAM(QWidget):
    """Gráfico de barras horizontais com cobertura de interferências por sistema hídrico.

    Renderiza via QPainter um conjunto de barras horizontais empilhadas
    verticalmente, uma por sistema hídrico. Cada barra é composta por:
        - **Fundo cinza** (``#e0e0e0``): representa 100 % das interferências
          obrigadas para o sistema;
        - **Barra colorida**: avança proporcionalmente ao percentual cadastrado,
          com a mesma escala de cores do ``GaugeSemicircular``
          (verde ≥ 70 %, amarelo ≥ 40 %, vermelho < 40 %);
        - **Texto absoluto** (``cadastrados/obrigados``): posicionado à direita
          da barra colorida, dentro da área cinza, para facilitar a leitura
          exata quando o preenchimento é baixo;
        - **Percentual** (ex.: ``42%``): exibido à direita de toda a barra.

    Os nomes dos sistemas hídricos são truncados com reticências (elidedText)
    quando ultrapassam a margem esquerda reservada (160 px).

    A altura do widget é calculada dinamicamente em ``set_dados`` para
    acomodar todas as linhas sem corte (28 px por sistema + 40 px de margem),
    tornando o componente compatível com ``QScrollArea``.

    Attributes:
        _dados (list[tuple[str, int, int]]): Lista de tuplas no formato
            ``(nome_sistema, total_obrigados, total_cadastrados)`` fornecida
            pela query ``SQL_SISHIDRICO`` do ``WidgetDashboard``.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._dados = []   # lista de (sistema, obrigados, cadastrados)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

    def set_dados(self, dados):
        """Recebe lista de (sistema, obrigados, cadastrados) e redesenha."""
        self._dados = dados if dados else []
        # Altura dinâmica: 28px por linha + margens
        altura = max(180, len(self._dados) * 28 + 40)
        self.setFixedHeight(altura)
        self.update()

    def paintEvent(self, event):
        if not self._dados:
            return

        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        w      = self.width()
        mar_l  = 160   # espaço para o nome do sistema
        mar_r  = 52    # espaço para o valor percentual
        mar_t  = 8
        alt_barra = 16
        esp       = 12  # espaçamento entre barras
        area_w    = w - mar_l - mar_r

        font_nome = QFont()
        font_nome.setPointSize(8)
        font_val  = QFont()
        font_val.setPointSize(8)
        font_val.setBold(True)

        for i, (sistema, obrigados, cadastrados) in enumerate(self._dados):
            y = mar_t + i * (alt_barra + esp)

            pct = (cadastrados / obrigados * 100) if obrigados > 0 else 0.0

            # Cor conforme percentual
            if pct >= 70:
                cor = QColor("#1cc88a")
            elif pct >= 40:
                cor = QColor("#f6c23e")
            else:
                cor = QColor("#e74a3b")

            # --- Nome do sistema (truncado se necessário) ---
            painter.setFont(font_nome)
            painter.setPen(QColor("#444"))
            rect_nome = QRectF(0, y, mar_l - 8, alt_barra)
            nome_curto = painter.fontMetrics().elidedText(
                sistema, Qt.ElideRight, int(mar_l - 8)
            )
            painter.drawText(rect_nome, Qt.AlignVCenter | Qt.AlignRight, nome_curto)

            # --- Barra de fundo (cinza) ---
            painter.setPen(Qt.NoPen)
            painter.setBrush(QColor("#e0e0e0"))
            painter.drawRoundedRect(
                QRectF(mar_l, y, area_w, alt_barra), 4, 4
            )

            # --- Barra de progresso ---
            larg_prog = area_w * min(pct / 100, 1.0)
            if larg_prog > 0:
                painter.setBrush(cor)
                painter.drawRoundedRect(
                    QRectF(mar_l, y, larg_prog, alt_barra), 4, 4
                )

            # --- Texto absoluto dentro da área cinza (à direita da barra colorida) ---
            txt_abs = f"{cadastrados}/{obrigados}"
            painter.setFont(font_val)
            fm = painter.fontMetrics()
            txt_w = fm.horizontalAdvance(txt_abs)
            x_txt = mar_l + 4   # alinhado à esquerda dentro da barra, com pequeno recuo
            # garante que não sobreponha a barra colorida
            x_txt = max(x_txt, mar_l + larg_prog + 3)
            painter.setPen(QColor("#ffffff"))
            painter.drawText(
                QRectF(x_txt, y, txt_w + 2, alt_barra),
                Qt.AlignVCenter | Qt.AlignLeft,
                txt_abs
            )

            # --- Percentual à direita ---
            painter.setFont(font_val)
            painter.setPen(QColor("#333"))
            painter.drawText(
                QRectF(mar_l + area_w + 4, y, mar_r - 4, alt_barra),
                Qt.AlignVCenter | Qt.AlignLeft,
                f"{pct:.0f}%"
            )

        painter.end()
    


class WidgetDashboard(QWidget):
    """Painel de resumo executivo (dashboard) do sistema DURH Diária por Telemetria.

    Consolida em uma única tela os principais indicadores quantitativos do
    sistema, apresentando-os em três camadas visuais complementares:

        1. **Cards de métricas** — cinco contadores absolutos (Operadores, Empreendimentos,
           Usuários, Interferências e Medidores), exibidos com ícone, valor
           numérico em destaque e rótulo categórico.
        2. **Gauges semicirculares** — dois velocímetros em arco de 180° que
           representam, respectivamente, o percentual de empreendimentos cadastrados
           em relação ao total de obrigados e o percentual de interferências
           cadastradas em relação ao total de obrigadas. A cor do arco varia
           entre vermelho (< 40 %), amarelo (40–69 %) e verde (≥ 70 %).
        3. **Gráfico de barras horizontais** — exibe a proporção
           cadastrados/obrigados por unidade de automonitoramento, com nomes truncados
           e valores absolutos sobrepostos na barra, rolável verticalmente
           quando o número de sistemas for grande.

    Todas as consultas são executadas no método ``carregar_dados()`` logo
    após a construção do layout (``initUI``). Em caso de falha em qualquer
    query, todos os cards são marcados com "—" e uma mensagem de erro é
    exibida ao usuário.

    As queries SQL são declaradas como atributos de classe (``SQL_*``) para
    facilitar testes unitários e reutilização sem instanciar o widget.

    Attributes:
        conn (psycopg2.connection): Conexão ativa ao banco PostgreSQL.
        usuario_logado (str | None): Usuário da sessão atual (reservado para
            futuras restrições de visibilidade por perfil).
        _cards (dict[str, QLabel]): Mapeamento ``chave → QLabel`` dos valores
            numéricos exibidos nos cards. Chaves: ``"operadores"``,
            ``"usuarios"``, ``"interferencias"``, ``"medidores"``.
        _gauge_empreendimentos (GaugeSemicircular): Gauge de cobertura de usuários.
        _gauge_interf (GaugeSemicircular): Gauge de cobertura de interferências.
        _dados_uam (list[tuple]): Cache do resultado da query
            ``SQL_UAM`` no formato ``(nome_uam, obrigados, cadastrados)``.
        _grafico (GraficoUAM): Widget de barras horizontais por unidades de
            automonitoramento.

    Class Attributes:
        SQL_UAM (str): Query que retorna contagem de interferências
            obrigadas e cadastradas agrupadas por unidade de automonitoramento,
            com abreviação automática de nomes longos (Demais UGRHs...).
        SQL_OPERADORES (str): Contagem total de operadores na
            ``tb_operador_telemetria``.
        SQL_USUARIOS_CAD (str): Contagem de usuários com ao menos um
            medidor cadastrado em ``view_ft_intervencao``.
        SQL_USUARIOS_OBR (str): Contagem de usuários obrigados à telemetria
            em ``view_ft_captacao_obrigatoriedade``.
        SQL_INTERF_CAD (str): Contagem de interferências distintas com
            medidor cadastrado.
        SQL_INTERF_OBR (str): Contagem total de interferências obrigadas.
        SQL_MEDIDORES (str): Contagem de medidores ativos com geolocalização,
            excluindo registros de teste (rótulo ``999``, ``VERDE GRANDE``)
            e o operador interno de ID 162.
    """
    
    SQL_UAM = """
        SELECT
            CASE 
                WHEN s.nome_uam LIKE 'Demais UGRHs%%' 
                THEN REPLACE(s.nome_uam, 'Demais UGRHs Federais e UGRHs Estaduais', 'Demais UGRH Fed. Est.')
                WHEN s.nome_uam LIKE 'Marcos%%'
                THEN REPLACE(s.nome_uam, 'Marcos Regulatórios no Semiárido', 'Marcos Reg. Semiárido')
            ELSE s.nome_uam
            END AS nome_uam,
            COUNT(DISTINCT s.codigo_interferencia) AS obrigados,
            COUNT(DISTINCT CASE WHEN s.cadastrado = 'sim' 
                           THEN s.codigo_interferencia END) AS cadastrados
        FROM view_ft_captacao_obrigatoriedade s
        GROUP BY s.nome_uam
        HAVING COUNT(DISTINCT s.codigo_interferencia) > 0
        ORDER BY s.nome_uam;
    """

    SQL_OPERADORES   = "SELECT COUNT(*) FROM tb_operador_telemetria;"
    SQL_USUARIOS_CAD = "SELECT COUNT(DISTINCT nome_usuario) FROM view_ft_intervencao;"
    SQL_USUARIOS_OBR = "SELECT COUNT(DISTINCT nome_usuario) FROM view_ft_captacao_obrigatoriedade;"
    SQL_EMPR_CAD = "SELECT COUNT(DISTINCT nu_cnarh) FROM view_ft_intervencao;"
    SQL_EMPR_OBR = "SELECT COUNT(DISTINCT numero_cadastro) FROM view_ft_captacao_obrigatoriedade;"    
    SQL_INTERF_CAD   = "SELECT COUNT(DISTINCT nu_interferencia_cnarh) FROM view_ft_intervencao;"
    SQL_INTERF_OBR   = "SELECT COUNT(*) FROM view_ft_captacao_obrigatoriedade;"
    SQL_MEDIDORES    = """
        SELECT COUNT(*) FROM tb_intervencao
        WHERE rotulo !~~ '%%999%%'::text
          AND rotulo !~~ 'VERDE GRANDE%%'::text
          AND rotulo !~~ '%%#'::text          
          AND longitude IS NOT NULL
          AND latitude  IS NOT NULL;
    """

    def __init__(self, conexao, usuario=None):
        super().__init__()
        self.conn           = conexao
        self.usuario_logado = usuario
        self._cards         = {}
        self._gauge_empreendimentos = None
        self._gauge_interf      = None
        self._dados_sishidrico  = []
        self.initUI()
        self.carregar_dados()

    def initUI(self):
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(8, 8, 8, 8)

        content_widget = QWidget()
        content_widget.setStyleSheet("""
            QWidget {
                background-color: white;
                border: 1px solid #ccc;
                border-radius: 5px;
            }
        """)

        inner = QVBoxLayout(content_widget)
        inner.setContentsMargins(20, 16, 20, 16)
        inner.setSpacing(14)

        # --- Linha de cards ---
        cards_layout = QHBoxLayout()
        cards_layout.setSpacing(12)

        definicoes = [
            ("operadores",     "👥", "OPERADORES"),
            ("usuarios",       "👤", "USUÁRIOS"),
            ("empreendimentos", "🏭", "EMPREENDIMENTOS"),
            ("interferencias", "〰", "INTERFERÊNCIAS"),
            ("medidores",      "⏲", "MEDIDORES"),
        ]

        for chave, icone, rotulo in definicoes:
            card, lbl_valor = self._criar_card(icone, "...", rotulo)
            self._cards[chave] = lbl_valor
            cards_layout.addWidget(card)

        inner.addLayout(cards_layout)

        # --- Separador ---
        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setStyleSheet("border: none; background-color: #e0e0e0; max-height: 1px;")
        inner.addWidget(sep)

        # --- Linha de gauges ---
        gauges_layout = QHBoxLayout()
        gauges_layout.setSpacing(20)

        self._gauge_empreendimentos = GaugeSemicircular("Empreendimentos cadastrados")
        self._gauge_interf   = GaugeSemicircular("Interferências cadastradas")

        gauges_layout.addStretch(1)
        gauges_layout.addWidget(self._gauge_empreendimentos)
        gauges_layout.addStretch(1)
        gauges_layout.addWidget(self._gauge_interf)
        gauges_layout.addStretch(1)

        inner.addLayout(gauges_layout)

        # --- Separador 2 ---
        sep2 = QFrame()
        sep2.setFrameShape(QFrame.HLine)
        sep2.setStyleSheet("border: none; background-color: #e0e0e0; max-height: 1px;")
        inner.addWidget(sep2)

        # --- Gráfico barras horizontais: % cadastrado por unidade de automonitoramento ---
        lbl_graf = QLabel("Interferências por Unidade de Automonitoramento  (%  cadastrado / obrigado)")
        lbl_graf.setStyleSheet(
            "font-size: 12px; font-weight: bold; color: #555; border: none;"
        )
        inner.addWidget(lbl_graf)

        self._grafico = GraficoUAM()

        scroll = QScrollArea()
        scroll.setWidget(self._grafico)
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll.setFixedHeight(180)
        scroll.setStyleSheet("""
            QScrollArea {
                border: 1px solid #e0e0e0;
                border-radius: 4px;
                background-color: white;
            }
            QScrollBar:vertical {
                width: 8px;
                background: #f0f0f0;
                border-radius: 4px;
            }
            QScrollBar::handle:vertical {
                background: #c0c0c0;
                border-radius: 4px;
                min-height: 20px;
            }
            QScrollBar::handle:vertical:hover {
                background: #a0a0a0;
            }
            QScrollBar::add-line:vertical,
            QScrollBar::sub-line:vertical {
                height: 0px;
            }
        """)
        inner.addWidget(scroll)
        
        main_layout.addWidget(content_widget)
        self.setLayout(main_layout)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

    def _criar_card(self, icone, valor, rotulo):
        """Cria e retorna (card_widget, lbl_valor) para um card de métrica."""
        card = QFrame()
        card.setFrameShape(QFrame.StyledPanel)
        card.setStyleSheet("""
            QFrame {
                background-color: #f8f9fc;
                border: 1px solid #e0e0e0;
                border-radius: 8px;
            }
        """)
        card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        card.setFixedHeight(120)

        layout = QVBoxLayout(card)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(4)
        layout.setAlignment(Qt.AlignCenter)

        lbl_icone = QLabel(icone)
        lbl_icone.setAlignment(Qt.AlignCenter)
        lbl_icone.setStyleSheet("font-size: 26px; border: none; color: #4e73df;")

        lbl_valor = QLabel(valor)
        lbl_valor.setAlignment(Qt.AlignCenter)
        lbl_valor.setStyleSheet(
            "font-size: 20px; font-weight: bold; color: #2c2c2c; border: none;"
        )

        lbl_rotulo = QLabel(rotulo)
        lbl_rotulo.setAlignment(Qt.AlignCenter)
        lbl_rotulo.setStyleSheet(
            "font-size: 9px; color: #888; letter-spacing: 1px; border: none;"
        )

        layout.addWidget(lbl_icone)
        layout.addWidget(lbl_valor)
        layout.addWidget(lbl_rotulo)

        return card, lbl_valor

    def carregar_dados(self):
        """Executa todas as queries e atualiza cards e gauges."""
        try:
            cursor = self.conn.cursor()

            # Card 1 — Operadores
            cursor.execute(self.SQL_OPERADORES)
            self._cards["operadores"].setText(str(cursor.fetchone()[0]))

            # Card 2 — Usuários
            cursor.execute(self.SQL_USUARIOS_CAD)
            usuarios_cad = cursor.fetchone()[0]
            cursor.execute(self.SQL_USUARIOS_OBR)
            usuarios_obr = cursor.fetchone()[0]           
            self._cards["usuarios"].setText(f"{usuarios_cad}/{usuarios_obr}")

            # Card 3 — Empreendimentos
            cursor.execute(self.SQL_EMPR_CAD)
            empr_cad = cursor.fetchone()[0]
            cursor.execute(self.SQL_EMPR_OBR)
            empr_obr = cursor.fetchone()[0]            
            self._cards["empreendimentos"].setText(f"{empr_cad}/{empr_obr}")
            self._gauge_empreendimentos.set_valor(empr_cad, empr_obr)

            # Card 4 — Interferências
            cursor.execute(self.SQL_INTERF_CAD)
            interf_cad = cursor.fetchone()[0]
            cursor.execute(self.SQL_INTERF_OBR)
            interf_obr = cursor.fetchone()[0]
            self._cards["interferencias"].setText(f"{interf_cad}/{interf_obr}")
            self._gauge_interf.set_valor(interf_cad, interf_obr)

            # Card 5 — Medidores
            cursor.execute(self.SQL_MEDIDORES)
            self._cards["medidores"].setText(str(cursor.fetchone()[0]))

            # Gráfico — Sistemas Hídricos (query mais pesada, cursor de espera)
            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                cursor.execute(self.SQL_UAM)
                self._dados_uam = cursor.fetchall()
            finally:
                QApplication.restoreOverrideCursor()

            self._grafico.set_dados(self._dados_uam)

            cursor.close()

        except Exception as e:
            for lbl in self._cards.values():
                lbl.setText("—")
            self._gauge_empreendimentos.set_valor(0, 1)
            self._gauge_interf.set_valor(0, 1)
            QMessageBox.critical(
                self, "Erro", f"Erro ao carregar dados do Dashboard:\n{e}"
            )
   
