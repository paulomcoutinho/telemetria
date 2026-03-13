# -*- coding: utf-8 -*-
"""
Módulo: tela_cadastro_medidores.py
====================================
Formulário multi-etapa de cadastro de medidores de vazão no banco
de dados DURH Diária (tabela tb_intervencao).

Funcionalidades principais:
  - Vinculação a operadores cadastrados (FK tb_operador_telemetria);
  - Conversão automática de unidades (m³/h → m³/s; cv → kW);
  - Georreferenciamento manual ou por captura direta no canvas QGIS;
  - Exibição do medidor cadastrado como camada vetorial pontual no canvas;
  - Suporte a cadastro único e múltiplo por sessão.

Autor : SFI/ANA
Versão: 2.0 – Março/2026
"""

# ---------------------------------------------------------------------------
# Imports Qt – widgets, layout e utilitários de UI
# ---------------------------------------------------------------------------
from qgis.PyQt.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QComboBox, QCheckBox,
    QTreeWidget, QTreeWidgetItem, QFrame, QMessageBox,
    QSizePolicy, QSpacerItem, QScrollArea, QStackedWidget,
    QAbstractItemView, QRadioButton, QDesktopWidget,
    QDialog, QDialogButtonBox, QListWidget, QTextEdit,
    QApplication,
)

from qgis.PyQt.QtCore import Qt, QTimer, QVariant, QRegExp, QMimeData
    
from qgis.PyQt.QtGui import QDoubleValidator, QColor, QRegExpValidator

# ---------------------------------------------------------------------------
# Imports QGIS core
# ---------------------------------------------------------------------------
from qgis.core import (
    QgsProject, QgsVectorLayer, QgsFields, QgsField, QgsFeature, QgsGeometry,
    QgsPointXY, QgsMarkerSymbol, QgsSingleSymbolRenderer, QgsCoordinateTransform,
    QgsCoordinateReferenceSystem, QgsWkbTypes, QgsDataSourceUri, QgsVectorDataProvider,
    QgsPalLayerSettings, QgsVectorLayerSimpleLabeling, QgsTextFormat, QgsUnitTypes,
    QgsLayerTreeLayer, QgsRectangle, QgsCategorizedSymbolRenderer, QgsRendererCategory,    
    QgsRasterLayer,
)

from qgis.utils import iface
from qgis.gui import QgsMapCanvas, QgsMapToolIdentifyFeature, QgsMapToolIdentify

# ---------------------------------------------------------------------------
# Imports Python padrão
# ---------------------------------------------------------------------------
import psycopg2
import urllib.request
import urllib.parse
import ssl
import json
import re

# ---------------------------------------------------------------------------
# Sistema de design centralizado
# ---------------------------------------------------------------------------
try:
    from . import ui_tema
except ImportError:
    import ui_tema

# ---------------------------------------------------------------------------
# Diálogos auxiliares de confirmação de unidade
# ---------------------------------------------------------------------------
from .dialogo_unidade_vazao    import DialogoUnidadeVazao
from .dialogo_unidade_potencia import DialogoUnidadePotencia


class TelaCadastroMedidores(QWidget):
    """Tela para cadastro de medidores de volume e vazão no sistema DURH Diária.
    
    Esta tela permite o cadastro de novos medidores associados a interferências outorgadas,
    suportando tanto cadastro único quanto múltiplo de medidores por interferência.
    A interface é dinâmica, adaptando-se ao tipo de operação selecionada pelo usuário.

    Attributes:
        tela_inicial (QWidget): Referência à tela inicial do plugin.
        conn (psycopg2.connection): Conexão ativa com o banco de dados PostgreSQL.
        medidor_atual (int): Contador do medidor atual em cadastros múltiplos (1-based).
        quantidade_medidores (int): Quantidade total de medidores a cadastrar.
        medidores_cadastrados (list): Lista de IDs de medidores cadastrados na sessão atual.
        nome_usuario (str): Nome do usuário titular da outorga.
        cnarh_encontrado (str): Código CNARH encontrado na consulta.
        numero_outorga (str): Número da outorga encontrado na consulta.
        id_operador (int): ID do operador de telemetria selecionado.
        _em_atualizacao (bool): Flag indicando modo de atualização (True/False).
        _em_cadastro_multiplo (bool): Flag indicando cadastro múltiplo em andamento.
        _dados_sequencia (dict): Dados temporários para cadastro múltiplo contendo:
            - sigla_base (str): Sigla da unidade de automonitoramento
            - numero_base (int): Número base para geração de rótulos
            - id_interferencia (int): ID da interferência associada
            - medidores (list): IDs dos medidores cadastrados
        limpando_campos (bool): Flag para controle interno durante limpeza de campos.

    Widgets Principais:
        operador_combo (QComboBox): Seleção de operadores cadastrados.
        cnarh_input (QLineEdit): Campo para código CNARH do usuário.
        nome_usuario_input (QLineEdit): Campo com nome do usuário (read-only).
        outorga_tree (QTreeWidget): Lista de outorgas disponíveis com checkboxes.
        tree_widget (QTreeWidget): Lista de interferências disponíveis com checkboxes.
        codigo_medidor_input (QLineEdit): Campo para código do medidor de energia.
        vazao_input (QLineEdit): Campo para vazão nominal da bomba (m³/s).
        potencia_input (QLineEdit): Campo para potência nominal da bomba (kW).
        equipamento_combo (QComboBox): Seleção de tipos de equipamento.
        modo_transmissao_combo (QComboBox): Seleção de modos de transmissão.
        cadastrar_btn (QPushButton): Botão para acionar o cadastro.
        voltar_btn (QPushButton): Botão para voltar à tela inicial.
    """
    def __init__(self, tela_inicial, conexao, usuario=None, senha=None):
        """Inicializa a tela de cadastro de medidores com design moderno."""   
        super().__init__()
        self.tela_inicial = tela_inicial
        self.setWindowTitle("Cadastro de Medidor - DURH Diária por Telemetria")
        self.setFixedSize(675, 880)        
        self.conn = conexao
        self.usuario_logado = usuario
        self.senha_conexao = senha        
        self.center()
        
        ui_tema.aplicar_tema_arredondado(self)

        # Layout principal com ScrollArea para janelas longas
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(15, 15, 15, 15)
        
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        
        content_widget = QWidget()
        layout = QVBoxLayout(content_widget)
        #layout.setSpacing(15)
        
        # --- SEÇÃO 1: PESQUISA ---
        pesquisa_container = QFrame()
        pesquisa_container.setObjectName("ContainerBranco")
        pesquisa_layout = QVBoxLayout(pesquisa_container)
        pesquisa_layout.setContentsMargins(20, 20, 20, 20)
        
        titulo_pesquisa = QLabel("1. Pesquisa por Interferência")
        titulo_pesquisa.setStyleSheet(f"color: {ui_tema.StyleConfig.PRIMARY_COLOR}; font-weight: bold; font-size: 14px;")
        pesquisa_layout.addWidget(titulo_pesquisa)
        
        self.operador_label = QLabel("Operador de Telemetria:")
        self.operador_combo = QComboBox()
        self.carregar_operadores()
        pesquisa_layout.addWidget(self.operador_label)
        pesquisa_layout.addWidget(self.operador_combo)

        self.cnarh_label = QLabel("Código CNARH:")
        self.cnarh_input = QLineEdit()
        self.cnarh_input.setPlaceholderText("Ex: 310005248163")
        self.cnarh_input.setMaxLength(15)
        self.cnarh_input.setValidator(QRegExpValidator(QRegExp("^[0-9]{0,15}$"), self))
        pesquisa_layout.addWidget(self.cnarh_label)
        pesquisa_layout.addWidget(self.cnarh_input)

        self.nome_usuario_label = QLabel("Titular da Outorga:")
        self.nome_usuario_input = QLineEdit()
        self.nome_usuario_input.setDisabled(True)
        pesquisa_layout.addWidget(self.nome_usuario_label)
        pesquisa_layout.addWidget(self.nome_usuario_input)

        self.outorga_label = QLabel("Selecionar Outorga:")
        self.outorga_label.setVisible(False)
        self.outorga_tree = QTreeWidget()
        self.outorga_tree.setHeaderLabels([
            "Resolução ANA", 
            "Vencimento", 
            "Finalidade", 
            "Qmed (m³/h)", 
            "Qmax (m³/h)", 
            "Selecione"
        ])
        self.outorga_tree.setColumnWidth(0, 100)  # Resolução ANA
        self.outorga_tree.setColumnWidth(1, 95)  # Vencimento
        self.outorga_tree.setColumnWidth(2, 100)  # Finalidade
        self.outorga_tree.setColumnWidth(3, 90)   # Qmed
        self.outorga_tree.setColumnWidth(4, 90)   # Qmax
        self.outorga_tree.setColumnWidth(5, 80)   # Selecione (checkbox)        
        self.outorga_tree.setVisible(False)
        pesquisa_layout.addWidget(self.outorga_label)
        pesquisa_layout.addWidget(self.outorga_tree)

        self.int_btn = QPushButton("Pesquisar Interferências")
        self.int_btn.clicked.connect(self.buscar_interferencia)
        pesquisa_layout.addWidget(self.int_btn)
        
        layout.addWidget(pesquisa_container)

        # --- SEÇÃO 2: DADOS DO MEDIDOR ---
        medidor_container = QFrame()
        medidor_container.setObjectName("ContainerBranco")
        medidor_layout = QVBoxLayout(medidor_container)
        medidor_layout.setContentsMargins(20, 20, 20, 20)

        # Título e Subtítulo em linha horizontal
        header_medidor_layout = QHBoxLayout()
        
        titulo_medidor = QLabel("2. Dados do Medidor")
        titulo_medidor.setStyleSheet(f"color: {ui_tema.StyleConfig.PRIMARY_COLOR}; font-weight: bold; font-size: 14px;")
        header_medidor_layout.addWidget(titulo_medidor)

        self.subtitulo_medidor = QLabel("Medidor")
        self.subtitulo_medidor.setStyleSheet(f"color: {ui_tema.StyleConfig.PRIMARY_COLOR}; font-weight: bold; font-size: 14px;")
        self.subtitulo_medidor.setVisible(False) # Inicialmente invisível (Sim é padrão)
        header_medidor_layout.addWidget(self.subtitulo_medidor)
        
        header_medidor_layout.addStretch()
        medidor_layout.addLayout(header_medidor_layout)

        self.pergunta_label = QLabel("Possui apenas um medidor para múltiplas outorgas?")
        medidor_layout.addWidget(self.pergunta_label)
        
        radio_layout = QHBoxLayout()
        self.radio_sim = QRadioButton("Sim")
        self.radio_nao = QRadioButton("Não")
        self.radio_sim.setChecked(True)
        self.radio_nao.toggled.connect(self.mostrar_quantidade_medidores)
        radio_layout.addWidget(self.radio_sim)
        radio_layout.addWidget(self.radio_nao)
        radio_layout.addStretch()
        medidor_layout.addLayout(radio_layout)

        self.quantidade_medidor_label = QLabel("Quantidade de medidor por interferência:")
        self.quantidade_medidor_label.setVisible(False)
        self.quantidade_medidor_combo = QComboBox()
        self.quantidade_medidor_combo.addItems([str(i) for i in range(2, 11)])
        self.quantidade_medidor_combo.setVisible(False)
        medidor_layout.addWidget(self.quantidade_medidor_label)
        medidor_layout.addWidget(self.quantidade_medidor_combo)

        self.interferencia_label = QLabel("Selecionar interferência(s) outorgada(s):")
        medidor_layout.addWidget(self.interferencia_label)
        self.tree_widget = QTreeWidget()
        self.tree_widget.setHeaderLabels(["INT_CD", "Selecione"])
        self.tree_widget.setSelectionMode(QTreeWidget.MultiSelection)
        self.tree_widget.setMinimumHeight(120)
        medidor_layout.addWidget(self.tree_widget)

        self.codigo_medidor_label = QLabel("Código do Relógio (Energia):")
        self.codigo_medidor_input = QLineEdit()
        self.codigo_medidor_input.setPlaceholderText("Digite código do relógio")
        medidor_layout.addWidget(self.codigo_medidor_label)
        medidor_layout.addWidget(self.codigo_medidor_input)

        # Grid para Vazão e Potência
        grid_tecnico = QGridLayout()
        self.vazao_label = QLabel("Vazão convertida para m³/s:")
        self.vazao_input = QLineEdit()
        self.vazao_input.setPlaceholderText("Digite valor total em m³/h")
        self.vazao_input.editingFinished.connect(self.converter_vazao_para_m3s)
        
        self.potencia_label = QLabel("Potência convertida para kW:")
        self.potencia_input = QLineEdit()
        self.potencia_input.setPlaceholderText("Digite valor total em cv")
        self.potencia_input.editingFinished.connect(self.converter_potencia_para_kw)
        
        grid_tecnico.addWidget(self.vazao_label, 0, 0)
        grid_tecnico.addWidget(self.vazao_input, 1, 0)
        grid_tecnico.addWidget(self.potencia_label, 0, 1)
        grid_tecnico.addWidget(self.potencia_input, 1, 1)
        medidor_layout.addLayout(grid_tecnico)

        self.equipamento_label = QLabel("Equipamento de Medição:")
        self.equipamento_combo = QComboBox()
        self.carregar_equipamentos()
        medidor_layout.addWidget(self.equipamento_label)
        medidor_layout.addWidget(self.equipamento_combo)

        self.modo_transmissao_label = QLabel("Modo de Transmissão:")
        self.modo_transmissao_combo = QComboBox()
        self.carregar_modos_transmissao()
        medidor_layout.addWidget(self.modo_transmissao_label)
        medidor_layout.addWidget(self.modo_transmissao_combo)

        layout.addWidget(medidor_container)
        
        # Botões finais
        btn_layout = QHBoxLayout()
        self.cadastrar_btn = QPushButton("Cadastrar")
        self.cadastrar_btn.setDisabled(True)
        self.cadastrar_btn.clicked.connect(self.cadastrar_medidor)
        
        self.voltar_btn = QPushButton("Voltar")
        self.voltar_btn.setStyleSheet(f"background-color: transparent; color: {ui_tema.StyleConfig.SECONDARY_COLOR}; border: 1px solid {ui_tema.StyleConfig.SECONDARY_COLOR};")
        self.voltar_btn.clicked.connect(self.voltar)
        
        btn_layout.addStretch()
        btn_layout.addWidget(self.voltar_btn)
        btn_layout.addWidget(self.cadastrar_btn)
        layout.addLayout(btn_layout)

        scroll.setWidget(content_widget)
        main_layout.addWidget(scroll)
        
        self.setLayout(main_layout)


        # Conectar os sinais de alteração dos campos ao método verificar_campos
        self.cnarh_input.textChanged.connect(self.verificar_campos)
        self.operador_combo.currentIndexChanged.connect(self.verificar_campos)
        self.codigo_medidor_input.textChanged.connect(self.verificar_campos)
        self.vazao_input.textChanged.connect(self.verificar_campos)
        self.potencia_input.textChanged.connect(self.verificar_campos)
        self.equipamento_combo.currentIndexChanged.connect(self.verificar_campos)
        self.modo_transmissao_combo.currentIndexChanged.connect(self.verificar_campos)
        self.tree_widget.itemChanged.connect(self.verificar_campos)
        self.outorga_tree.itemChanged.connect(self.verificar_campos)        

        # Define o layout
        self.setLayout(layout)
        
        #Notificações de cadastro executado       
        self.notification = QLabel("", self)
        self.notification.setStyleSheet("""
            background-color: #4CAF50;
            color: white;
            padding: 8px;
            border-radius: 4px;
        """)
        self.notification.hide()        
        
        #Limpeza dos campos do sistema
        self.limpando_campos = False

        # Inicializa os atributos de controle
        self.medidor_atual = 1  # Contador de medidores
        self.quantidade_medidores = 1  # Valor padrão
        self.medidores_cadastrados = []  # Lista para armazenar múltiplos medidores
        self._em_atualizacao = False
        self._em_cadastro_multiplo = False
        self._dados_sequencia = None
        self.vazao_processada = False
        self.potencia_processada = False

    def mostrar_quantidade_medidores(self):
        """Atualiza a interface quando o tipo de cadastro (único/múltiplo) muda.
        
        Controla a visibilidade dos controles para quantidade de medidores e
        ajusta o texto dos rótulos e placeholders conforme o contexto.
        
        Triggers:
            - Mudança no estado dos radio buttons (Sim/Não)
        """
        if self.radio_nao.isChecked():
            #self.habilitar_selecao_unica(True)
            self.quantidade_medidores = int(self.quantidade_medidor_combo.currentText())
            self.medidor_atual = 1
           
            self.quantidade_medidor_label.setVisible(True)
            self.quantidade_medidor_combo.setVisible(True)
            
            self.atualizar_interface_medidor()
            
            self.quantidade_medidor_combo.currentIndexChanged.connect(self.atualizar_quantidade_medidores)
        else:  
            #self.habilitar_selecao_unica(False)
            self.quantidade_medidores = 1
            self.medidor_atual = 1
            self.quantidade_medidor_label.setVisible(False)
            self.quantidade_medidor_combo.setVisible(False)
            try:
                self.quantidade_medidor_combo.currentIndexChanged.disconnect()
            except TypeError:
                pass
            self.atualizar_interface_medidor()

    def atualizar_quantidade_medidores(self):
        """Atualiza a quantidade total quando o comboBox muda"""
        self.quantidade_medidores = int(self.quantidade_medidor_combo.currentText())
        self.atualizar_interface_medidor()

    def carregar_operadores(self):
        """Carrega a lista de operadores cadastrados no combobox.
        
        Consulta a tabela tb_operador_telemetria para obter nome e e-mail,
        formatando como "Nome (email)" para exibição.
        
        Raises:
            psycopg2.Error: Em caso de falha na consulta ao banco.
        """
        cursor = None  # Inicializa a variável cursor como None
        try:
            cursor = self.conn.cursor()
            query = "SELECT DISTINCT nome, email FROM tb_operador_telemetria ORDER BY nome ASC;"
            cursor.execute(query)
            operadores = cursor.fetchall()

            # Limpa o combobox antes de carregar os novos dados
            self.operador_combo.clear()

            # Adiciona os operadores ao combobox mostrando nome e email
            for nome, email in operadores:
                self.operador_combo.addItem(f"{nome} ({email})")
                
                # Opcional: armazenar o email como dado do item
                self.operador_combo.setItemData(self.operador_combo.count()-1, email)
        except Exception as e:
            print(f"Erro ao carregar operadores: {e}")
            QMessageBox.critical(self, "Erro", f"Erro ao carregar operadores: {e}") 
        finally:
            if cursor:
                cursor.close()

    def carregar_equipamentos(self):
        """Carrega os equipamentos de medição da tabela tb_tipo_medidor."""
        cursor = None  # Inicializa a variável cursor como None        
        try:
            cursor = self.conn.cursor()
            query = "SELECT descricao FROM tb_tipo_medidor;"
            cursor.execute(query)
            equipamentos = cursor.fetchall()

            # Limpa o combo antes de carregar os novos dados
            self.equipamento_combo.clear()

            # Adiciona as opções ao combo
            for equipamento in equipamentos:
                self.equipamento_combo.addItem(f"{equipamento[0]}")
        except Exception as e:
            print(f"Erro ao carregar equipamentos: {e}")
            QMessageBox.critical(self, "Erro", f"Erro ao carregar equipamentos: {e}")        
        finally:
            if cursor:
                cursor.close()

    def carregar_modos_transmissao(self):
        """Carrega os modos de transmissão da tabela tb_modo_transmissao."""
        cursor = None  # Inicializa a variável cursor como None
        try:
            cursor = self.conn.cursor()
            query = "SELECT descricao FROM tb_modo_transmissao;"
            cursor.execute(query)
            modos = cursor.fetchall()

            # Limpa o combo antes de carregar os novos dados
            self.modo_transmissao_combo.clear()

            # Adiciona as opções ao combo
            for modo in modos:
                self.modo_transmissao_combo.addItem(f"{modo[0]}")
        except Exception as e:
            print(f"Erro ao carregar modos de transmissão: {e}")
            QMessageBox.critical(self, "Erro", f"Erro ao carregar modos de transmissão: {e}")
        finally:
            if cursor:
                cursor.close()

    def center(self):
        """Centraliza a janela na tela."""
        screen_geometry = QDesktopWidget().screenGeometry()
        center_point = screen_geometry.center()
        frame_geometry = self.frameGeometry()
        frame_geometry.moveCenter(center_point)
        self.move(frame_geometry.topLeft())
    
    def buscar_interferencia(self):
        """Busca interferências associadas ao CNARH e lista as outorgas disponíveis.
        
        Fluxo principal:
        1. Valida preenchimento do campo CNARH obrigatório
        2. Consulta nome do usuário e outorgas na tb_mapserver_obrigatoriedade
        3. Valida operador selecionado
        4. Lista outorgas disponíveis em tree widget
        5. Busca códigos de interferência correspondentes à outorga selecionada
        6. Popula o tree widget com checkboxes de interferências
        
        Preenche automaticamente:
            - nome_usuario_input
            - cnarh_input (com código formatado)
            - outorga_tree (com lista de outorgas disponíveis)
            
        Raises:
            psycopg2.Error: Em caso de falha nas consultas SQL.
            ValueError: Se dados essenciais não forem encontrados.
        """
        cnarh = self.cnarh_input.text().strip()
        operador_selecionado = self.operador_combo.currentText().strip()

        # Verifica se o campo CNARH está preenchido
        if not cnarh:
            QMessageBox.warning(self, "Campo Vazio", "Preencha o campo 'Código CNARH'.")
            return
        
        if not operador_selecionado:
            QMessageBox.warning(self, "Campo Vazio", "Selecione um operador.")
            return

        # Extrai nome e email do texto do ComboBox (formato: "Nome (email)")
        try:
            operador_nome, operador_email = operador_selecionado.split(" (")
            operador_email = operador_email.rstrip(")")
        except Exception as e:
            QMessageBox.warning(self, "Formato inválido", f"Erro ao processar operador: {e}")
            return

        cursor = None
        try:
            cursor = self.conn.cursor()

            # Passo 1: Busca o nome do usuário e outorgas na tabela tb_mapserver_obrigatoriedade
            query_usuario = """
            SELECT DISTINCT 
                nome_usuario, 
                numero_cadastro, 
                numero_resolucao,
                dr_vencimento_outorga,
                finalidade_outorga,
                vazao_media_m3_h,
                vazao_maxima_m3_h
            FROM tb_mapserver_obrigatoriedade
            WHERE numero_cadastro = %s
            ORDER BY numero_resolucao;
            """
            cursor.execute(query_usuario, (cnarh,))
            resultados = cursor.fetchall()

            if not resultados:
                QMessageBox.warning(
                    self,
                    "Dados não encontrados!!",
                    f"ATENÇÃO!!\n\n" 
                    f"Operador/usuário não encontrado na base de dados!\n"
                    f"Revise o número CNARH correto junto ao operador."
                )
                self.cnarh_input.clear()
                return

            # Armazena o nome do usuário no atributo da classe para uso posterior
            self.nome_usuario = resultados[0][0]  # Primeiro resultado, primeira coluna
            self.cnarh_encontrado = resultados[0][1]  # Código CNARH

            # Preenche o campo nome_usuario_input com o nome do usuário encontrado
            self.nome_usuario_input.setText(self.nome_usuario)
            self.cnarh_input.setText(self.cnarh_encontrado)

            # Passo 2: Listar outorgas disponíveis no tree widget
            self.outorga_tree.clear()
            self.outorga_label.setVisible(True)
            self.outorga_tree.setVisible(True)

            outorgas_unicas = set()
            for resultado in resultados:
                outorga = resultado[2]  # Número da outorga
                vencimento = resultado[3] if len(resultado) > 3 else None
                finalidade = resultado[4] if len(resultado) > 4 else None
                qmed = resultado[5] if len(resultado) > 5 else None
                qmax = resultado[6] if len(resultado) > 6 else None
                
                if outorga and outorga not in outorgas_unicas:
                    outorgas_unicas.add(outorga)
                    item = QTreeWidgetItem(self.outorga_tree)
                    item.setText(0, str(outorga))
                    
                    # Nova coluna 1: Vencimento
                    if vencimento:
                        try:
                            # Formatar data se for datetime
                            if hasattr(vencimento, 'strftime'):
                                item.setText(1, vencimento.strftime('%d/%m/%Y'))
                            else:
                                item.setText(1, str(vencimento)[:10])
                        except:
                            item.setText(1, str(vencimento))
                    else:
                        item.setText(1, "—")
                    
                    # Nova coluna 2: Finalidade
                    item.setText(2, str(finalidade) if finalidade else "—")
                    
                    # Nova coluna 3: Qmed (m³/s)
                    if qmed is not None:
                        try:
                            qmed_valor = float(qmed)
                            item.setText(3, f"{qmed_valor:.3f}".replace('.', ','))
                        except:
                            item.setText(3, str(qmed))
                    else:
                        item.setText(3, "—")
                    
                    # Nova coluna 4: Qmax (m³/s)
                    if qmax is not None:
                        try:
                            qmax_valor = float(qmax)
                            item.setText(4, f"{qmax_valor:.3f}".replace('.', ','))
                        except:
                            item.setText(4, str(qmax))
                    else:
                        item.setText(4, "—")

                    # Adiciona um QCheckBox na coluna 5 (última coluna)
                    checkbox = QCheckBox()
                    checkbox.stateChanged.connect(lambda state, cb=checkbox: self.desmarcar_outras_outorgas(cb))
                    checkbox.stateChanged.connect(self.buscar_interferencias_por_outorga)
                    self.outorga_tree.setItemWidget(item, 5, checkbox)

            # Passo 3: Valida o nome do operador selecionado
            query_operador = """
            SELECT id FROM tb_operador_telemetria
            WHERE nome = %s AND email = %s;
            """
            cursor.execute(query_operador, (operador_nome, operador_email))
            resultado_operador = cursor.fetchone()

            if not resultado_operador:
                QMessageBox.warning(
                    self,
                    "Operador não encontrado",
                    "O operador selecionado não foi encontrado na base de dados. Por favor, revise o nome do operador."
                )
                return

            self.id_operador = resultado_operador[0]  # Armazena o ID do operador para uso posterior

        except Exception as e:
            print(f"Erro ao buscar interferência: {e}")
            QMessageBox.critical(self, "Erro", f"Erro ao buscar interferência: {e}")
        finally:
            if cursor:
                cursor.close()

    def desmarcar_outras_outorgas(self, checkbox_atual):
        """
        Permite seleção múltipla de outorgas no tree widget.
        
        Este método foi ajustado para permitir que mais de uma outorga seja selecionada,
        removendo a lógica que desmarcava automaticamente as outras opções.
        
        Args:
            checkbox_atual (QCheckBox): Checkbox que disparou o evento.
        """
        # Não há necessidade de desmarcar outras checkboxes, pois agora permite múltiplas seleções
        pass

    def buscar_interferencias_por_outorga(self):
        """
        Busca e lista as interferências associadas às outorgas selecionadas.
        
        Este método é acionado quando uma ou mais outorgas são selecionadas no tree widget.
        Ele busca as interferências correspondentes às outorgas selecionadas E ao CNARH pesquisado.
        
        Fluxo:
        1. Identifica as outorgas selecionadas no tree widget "outorga_tree".
        2. Consulta o banco de dados para obter as interferências associadas às outorgas selecionadas
           FILTRANDO TAMBÉM PELO CNARH (numero_cadastro).
        3. Popula o tree widget "tree_widget" com as interferências encontradas.
        """
        # Passo 1: Identificar as outorgas selecionadas
        outorgas_selecionadas = []
        for i in range(self.outorga_tree.topLevelItemCount()):
            item = self.outorga_tree.topLevelItem(i)
            checkbox = self.outorga_tree.itemWidget(item, 5)
            if checkbox and checkbox.isChecked():
                outorgas_selecionadas.append(item.text(0))
        
        if not outorgas_selecionadas:
            self.tree_widget.clear()
            return
        
        cnarh_pesquisado = getattr(self, 'cnarh_encontrado', None)
        
        if not cnarh_pesquisado:
            QMessageBox.warning(
                self,
                "CNARH não encontrado",
                "CNARH não foi identificado na pesquisa. Por favor, realize a pesquisa novamente."
            )
            return
        
        cursor = None
        try:
            cursor = self.conn.cursor()
            
            # Passo 2: Consultar as interferências associadas às outorgas selecionadas
            placeholders = ','.join(['%s'] * len(outorgas_selecionadas))
            query_interferencias = f"""
                SELECT DISTINCT codigo_interferencia 
                FROM tb_mapserver_obrigatoriedade
                WHERE numero_resolucao IN ({placeholders})
                AND numero_cadastro = %s;
            """
            
            params = outorgas_selecionadas + [cnarh_pesquisado]
            cursor.execute(query_interferencias, params)
            resultados = cursor.fetchall()
            
            # Passo 3: Popular o tree widget com as interferências encontradas
            self.tree_widget.clear()
            if resultados:
                for resultado in resultados:
                    int_cd = resultado[0]
                    item = QTreeWidgetItem(self.tree_widget)
                    item.setText(0, str(int_cd))

                    # Adiciona um QCheckBox na segunda coluna
                    checkbox = QCheckBox()
                    checkbox.stateChanged.connect(self.verificar_campos)
                    self.tree_widget.setItemWidget(item, 1, checkbox)
            else:
                QMessageBox.warning(
                    self,
                    "Nenhuma interferência encontrada",
                    f"Nenhuma interferência foi encontrada para as outorgas selecionadas\n"
                    f"no CNARH {cnarh_pesquisado}."
                )
        
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Falha ao buscar interferências: {str(e)}")
        finally:
            if cursor:
                cursor.close()
                
    def atualizar_interface_medidor(self):
        """Atualiza todos os elementos da interface relacionados ao medidor atual"""
        # Atualiza o label principal
        if self.radio_nao.isChecked():
            # Exibe entre parênteses ao lado do título
            texto_subtitulo = f" (Medidor {self.medidor_atual} de {self.quantidade_medidores})"
            self.subtitulo_medidor.setText(texto_subtitulo)
            self.subtitulo_medidor.setVisible(True)
            
            self.codigo_medidor_input.setPlaceholderText(f"Digite código do relógio - Medidor {self.medidor_atual}")
            self.vazao_input.setPlaceholderText(f"Digite valor total em m³/h - Medidor {self.medidor_atual}")
            self.potencia_input.setPlaceholderText(f"Digite valor total em cv - Medidor {self.medidor_atual}")                  
        else:
            # Esconde o subtítulo quando "Sim" está selecionado
            self.subtitulo_medidor.setVisible(False)
            self.codigo_medidor_input.setPlaceholderText(f"Digite código do relógio")
            self.vazao_input.setPlaceholderText(f"Digite valor total em m³/h")
            self.potencia_input.setPlaceholderText(f"Digite valor total em cv") 
        
        # Atualiza o botão de cadastro
        self.cadastrar_btn.setText(
            f"Cadastrar MEDIDOR {self.medidor_atual}" if self.radio_nao.isChecked() 
            else "Cadastrar"
        )
      
        # Força a atualização da interface
        self.update()

    def desmarcar_outros_checkboxes(self, checkbox):
        """Desmarca outros checkboxes quando um é selecionado.
        
        Args:
            checkbox (QCheckBox): Checkbox que foi selecionado.
        """
        if checkbox.isChecked():  # Usa 'checkbox' em vez de 'checkbox_atual'
            for i in range(self.tree_widget.topLevelItemCount()):
                item = self.tree_widget.topLevelItem(i)
                other_checkbox = self.tree_widget.itemWidget(item, 1)
                if other_checkbox and other_checkbox.isChecked() and other_checkbox != checkbox:
                    other_checkbox.setChecked(False)

    def converter_vazao_para_m3s(self):
        """Converte valor de vazão de m³/h para m³/s com formatação condicional."""
        try:
            texto = self.vazao_input.text().strip().replace(',', '.')
            if not texto:
                # ✅ Se campo for limpo, resetar flag
                self.vazao_processada = False
                self.verificar_campos()
                return
            
            valor_m3h = float(texto)
            valor_m3s = valor_m3h / 3600

            # Formatação condicional: 2 casas se >= 0.1, senão 3 casas
            if valor_m3s >= 0.1:
                self.vazao_input.setText(f"{valor_m3s:.2f}")
            else:
                self.vazao_input.setText(f"{valor_m3s:.3f}")
            
            print(f"Convertido: {valor_m3h} m³/h → {valor_m3s} m³/s")
            
            # ✅ MARCAR como processado após conversão bem-sucedida
            self.vazao_processada = True
            
            # ✅ Chamar verificar_campos para atualizar estado do botão
            self.verificar_campos()

        except ValueError:
            # ✅ Em caso de erro, resetar flag
            self.vazao_processada = False
            self.verificar_campos()
            QMessageBox.warning(self, "Valor inválido", "Insira um número válido em m³/h para conversão.")
            
    def converter_potencia_para_kw(self):
        """Converte o valor de potência de cavalos (cv) para kW ao sair do campo."""
        try:
            texto = self.potencia_input.text().strip().replace(',', '.')
            if not texto:
                # ✅ Se campo for limpo, resetar flag
                self.potencia_processada = False
                self.verificar_campos()
                return

            valor_cv = float(texto)
            valor_kw = valor_cv * 0.7355
            
            # Arredonda e formata como inteiro
            self.potencia_input.setText(str(int(round(valor_kw))))
            
            print(f"Convertido: {valor_cv} cv → {int(round(valor_kw))} kW")
            
            # ✅ MARCAR como processado após conversão bem-sucedida
            self.potencia_processada = True
            
            # ✅ Chamar verificar_campos para atualizar estado do botão
            self.verificar_campos()
            
        except ValueError:
            # ✅ Em caso de erro, resetar flag
            self.potencia_processada = False
            self.verificar_campos()
            QMessageBox.warning(self, "Valor inválido", "Insira um número válido em cavalos para conversão.")
            
    def cadastrar_intervencao(self, codigo_interferencia, sigla_base=None, numero_base=None, medidor_num=None):
        """Cadastra uma intervenção (medidor) na tabela tb_intervencao.
        
        Args:
            codigo_interferencia (str): Código INT_CD da interferência.
            sigla_base (str, optional): Sigla geográfica para rótulo.
            numero_base (int, optional): Número sequencial para rótulo.
            medidor_num (int, optional): Número do medidor em cadastro múltiplo.
            
        Returns:
            int: ID da intervenção cadastrada ou None em caso de falha.
            
        Fluxo:
            1. Valida campos numéricos (vazão, potência)
            2. Obtém IDs dos comboboxes (modo transmissão, tipo medidor)
            3. Busca coordenadas geográficas
            4. Gera rótulo no formato SIGLA_NNN ou SIGLA_NNN_M
            5. Insere registro no banco
            
        Raises:
            ValueError: Para valores numéricos inválidos.
            psycopg2.Error: Em falhas de banco de dados.
        """
        cursor = None  # Inicializa a variável cursor como None
        try:
            # Validação dos campos numéricos
            vazao_text = self.vazao_input.text().strip()
            potencia_text = self.potencia_input.text().strip()
            # Obter interferências selecionadas
            interferencias_selecionadas = self.obter_interferencias_selecionadas()
            if not interferencias_selecionadas:
                return
            
            if not vazao_text:
                QMessageBox.warning(self, "Campo obrigatório", "O campo 'Vazão nominal da bomba' é obrigatório.")
                return None
                
            if not potencia_text:
                QMessageBox.warning(self, "Campo obrigatório", "O campo 'Potência nominal da bomba' é obrigatório.")
                return None

            if not sigla_base:
                sigla_base = self.obter_sigla_base(interferencias_selecionadas[0])

            # Validação do número base
            if numero_base is None:
                numero_base = 1  # Valor padrão se não fornecido
                QMessageBox.warning(self, "Aviso", "Número base não definido, utilizando valor padrão 1")

            # Conversão dos valores numéricos com tratamento de erro
            try:
                vazao = float(vazao_text)
                potencia = int(potencia_text)
            except ValueError as e:
                raise ValueError(f"Valor numérico inválido: {str(e)}")
            
            cursor = self.conn.cursor()

            # Passo 1: Obter o modo_transmissao_id a partir do self.modo_transmissao_combo
            modo_transmissao_descricao = self.modo_transmissao_combo.currentText()
            query_modo_transmissao = """
            SELECT id FROM tb_modo_transmissao WHERE descricao = %s;
            """
            cursor.execute(query_modo_transmissao, (modo_transmissao_descricao,))
            modo_transmissao_id = cursor.fetchone()[0]

            # Passo 2: Obter o tipo_medidor_id a partir do self.equipamento_combo
            tipo_medidor_descricao = self.equipamento_combo.currentText()
            query_tipo_medidor = """
            SELECT id FROM tb_tipo_medidor WHERE descricao = %s;
            """
            cursor.execute(query_tipo_medidor, (tipo_medidor_descricao,))
            tipo_medidor_id = cursor.fetchone()[0]

            # Passo 3: Obter as coordenadas da intervenção
            latitude, longitude = self.buscar_coordenadas([interferencias_selecionadas[0]])  # Busca coordenadas para a interferência selecionada
            if latitude is None or longitude is None:
                QMessageBox.warning(self, "Aviso", "Coordenadas não encontradas para a intervenção.")
                return None
         
            # Passo 5: Gerar o rótulo sequencial
            if self.radio_nao.isChecked() and medidor_num is not None and numero_base is not None:             
                rotulo = f"{sigla_base}_{numero_base:03d}_{medidor_num:01d}"
            else:
                rotulo = f"{sigla_base}_{numero_base:03d}"

            # Passo 6: Inserir os dados na tabela tb_intervencao, incluindo o rótulo
            query_intervencao = """
            INSERT INTO tb_intervencao (
                modo_transmissao_id, vazao_nominal, potencia, rotulo, tipo_medidor_id,
                operador_telemetria, material_tubulacao_id, espessura_tubulacao,
                diametro_tubulacao, latitude, longitude
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """
            valores_intervencao = (
                modo_transmissao_id,
                vazao,
                potencia,
                rotulo,
                tipo_medidor_id,
                self.id_operador,
                5,  # material_tubulacao_id (valor fixo)
                0,  # espessura_tubulacao (valor fixo)
                0,  # diametro_tubulacao (valor fixo)
                latitude,  # latitude
                longitude  # longitude
            )
            cursor.execute(query_intervencao, valores_intervencao)
            id_intervencao = cursor.fetchone()[0]

            # Confirma as alterações no banco de dados
            self.conn.commit()

           # Log de sucesso
            print(f"Intervenção cadastrada com sucesso. ID: {id_intervencao}, Rótulo: {rotulo}")
            return id_intervencao
        
        except ValueError as e:
            QMessageBox.warning(self, "Valor inválido", f"Por favor, verifique os valores numéricos:\n{e}")
            return None
        except Exception as e:
            print(f"Erro ao cadastrar intervenção: {e}")
            QMessageBox.critical(self, "Erro", f"Erro ao cadastrar intervenção: {e}")
            return None
        finally:
            if cursor:
                cursor.close()

    def cadastrar_codigo_uc_intervencao(self, id_intervencao, codigo_uc):
        try:
            cursor = self.conn.cursor()

            # Verifica se a intervenção existe
            cursor.execute("SELECT 1 FROM tb_intervencao WHERE id = %s", (id_intervencao,))
            if not cursor.fetchone():
                raise ValueError(f"Intervenção {id_intervencao} não existe em tb_intervencao")
                    
            cursor.execute("""
                INSERT INTO tb_codigo_uc_intervencao (intervencao_id, codigo_uc)
                VALUES (%s, %s)
            """, (id_intervencao, codigo_uc))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Erro ao inserir código_uc: {e}")
        finally:
            cursor.close()

    def upsert_interferencia(self, codigo_interferencia, nome_usuario=None, cnarh=None, codigo_uc_concat=None):
        """Insere ou atualiza registro na tabela tb_interferencia para múltiplas interferências.

        Args:
            codigo_interferencia (list): Lista de códigos de interferência.
            nome_usuario (str, optional): Nome do usuário.
            cnarh (str, optional): Código CNARH.
            codigo_uc_concat (str, optional): Código UC.

        Returns:
            list: Lista de IDs das interferências ou None em caso de erro.
        """
        cursor = None       
        try:
            # Garantir que codigo_interferencia seja uma lista
            if isinstance(codigo_interferencia, list):
                codigo_interferencia = ', '.join(codigo_interferencia)
                
            nome = (nome_usuario if nome_usuario is not None 
                   else getattr(self.nome_usuario_input, 'text', lambda: 'Não informado')())

            cnarh_val = (cnarh if cnarh is not None 
                        else getattr(self.cnarh_input, 'text', lambda: 'Não informado')())

            cursor = self.conn.cursor()

            # 1. Verifica se existe
            cursor.execute(
                "SELECT id FROM tb_interferencia WHERE codigo_interferencia = %s", 
                (codigo_interferencia,)
            )
            existe = cursor.fetchone()

            # 2. Obtem os codigos_uc distintos associados a essa interferencia via intervenções
            cursor.execute("""
                SELECT DISTINCT ci.codigo_uc
                FROM tb_codigo_uc_intervencao ci
                JOIN tb_intervencao_interferencia ii ON ii.intervencao_id = ci.intervencao_id
                JOIN tb_interferencia inf ON inf.id = ii.interferencia_id
                WHERE inf.codigo_interferencia = %s
            """, (codigo_interferencia,))
            ucs_distintos = [row[0] for row in cursor.fetchall() if row[0]]
            codigo_uc_concat = ', '.join(sorted(set(ucs_distintos))) if ucs_distintos else None

            # 3. Prepara valores
            valores = (
                nome,
                cnarh_val,
                codigo_uc_concat,
                codigo_interferencia
            )

            # 4. Executa UPSERT
            if existe:
                query = """
                UPDATE tb_interferencia 
                SET nome_usuario = %s,
                    numero_cadastro = %s,
                    codigo_uc = %s
                WHERE codigo_interferencia = %s
                RETURNING id
                """
            else:
                query = """
                INSERT INTO tb_interferencia 
                (nome_usuario, numero_cadastro, codigo_uc, codigo_interferencia)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """

            cursor.execute(query, valores)
            id_interferencia = cursor.fetchone()[0]
            self.conn.commit()
            return id_interferencia

        except Exception as e:
            self.conn.rollback()
            QMessageBox.critical(
                self, 
                "Erro na Interferência", 
                f"Falha ao registrar interferência {codigo_interferencia}:\n{str(e)}"
            )
            return None
        finally:
            if cursor:
                cursor.close()

    def buscar_coordenadas(self, codigos_interferencia):
        """Busca coordenadas na tabela tb_mapserver_obrigatoriedade.
        
        Args:
            codigos_interferencia (list): Lista de códigos de interferência.
            
        Returns:
            tuple: (latitude, longitude) ou (None, None) se não encontrado.
        """
        cursor = None  # Inicializa a variável cursor como None
        try:
            cursor = self.conn.cursor()
            for codigo_interferencia in codigos_interferencia:
                query = """
                SELECT latitude, longitude
                FROM tb_mapserver_obrigatoriedade
                WHERE codigo_interferencia = %s;
                """
                cursor.execute(query, (codigo_interferencia,))
                resultado = cursor.fetchone()

                if resultado:
                    latitude, longitude = resultado
                    print(f"Coordenadas encontradas para a interferência {codigo_interferencia}: Latitude = {latitude}, Longitude = {longitude}")
                    return latitude, longitude

            print("Nenhuma coordenada encontrada para as interferências selecionadas.")
            return None, None
        except Exception as e:
            print(f"Erro ao buscar coordenadas no PostgreSQL: {e}")
            return None, None
        finally:
            if cursor:
                cursor.close()
  
    def cadastrar_intervencao_interferencia(self, id_intervencao, id_interferencia):
        """Cadastra relação intervenção-interferência.
        
        Args:
            id_intervencao (int): ID da intervenção.
            id_interferencia (int): ID da interferência.
        """
        cursor = None  # Inicializa a variável cursor como None
        try:
            cursor = self.conn.cursor()
            query = """
            INSERT INTO tb_intervencao_interferencia (intervencao_id, interferencia_id)
            VALUES (%s, %s);
            """
            cursor.execute(query, (id_intervencao, id_interferencia))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Erro ao cadastrar intervenção-interferência: {e}")
        finally:
            cursor.close()

    def verificar_campos(self):
        """Verifica se todos os campos obrigatórios estão preenchidos."""
        if self.limpando_campos:
            return  # Não valida durante a limpeza dos campos

        # Verifica se há outorga selecionada
        outorga_selecionada = False
        for i in range(self.outorga_tree.topLevelItemCount()):
            item = self.outorga_tree.topLevelItem(i)
            # ✅ Coluna 5 (corrigido após adição das 4 novas colunas)
            checkbox = self.outorga_tree.itemWidget(item, 5)
            if checkbox and checkbox.isChecked():
                outorga_selecionada = True
                break

        # Verificação de interferências selecionadas
        interferencia_selecionada = False
        for i in range(self.tree_widget.topLevelItemCount()):
            item = self.tree_widget.topLevelItem(i)
            checkbox = self.tree_widget.itemWidget(item, 1)
            if checkbox and checkbox.isChecked():
                interferencia_selecionada = True
                break

        # Restante da lógica de validação
        campos_preenchidos = (
            self.cnarh_input.text().strip(),
            self.operador_combo.currentText().strip(),
            self.vazao_input.text().strip(),
            self.potencia_input.text().strip(),
            self.equipamento_combo.currentText().strip(),
            self.modo_transmissao_combo.currentText().strip(),
        )

        # ✅ NOVO: Só habilita se VAZÃO E POTÊNCIA foram processadas (desfocadas) E todos os campos preenchidos
        self.cadastrar_btn.setEnabled(
            all(campos_preenchidos) and 
            outorga_selecionada and 
            interferencia_selecionada and
            self.vazao_processada and      # ✅ Nova condição 1
            self.potencia_processada       # ✅ Nova condição 2
        )
        
    def verificar_interferencia_cadastrada(self, codigo_interferencia):
        """Verifica se interferência já tem medidor cadastrado.
        
        Args:
            codigo_interferencia (str): Código da interferência ou lista separada por vírgula.
            
        Returns:
            tuple: (id_interferencia, id_intervencao, rótulo) ou None.
        """
        cursor = None
        try:
            if self._em_atualizacao:
                return None
            
            cursor = self.conn.cursor()

            cursor.execute("""
                SELECT i.id, ii.intervencao_id, it.rotulo 
                FROM tb_interferencia i
                JOIN tb_intervencao_interferencia ii ON i.id = ii.interferencia_id
                JOIN tb_intervencao it ON ii.intervencao_id = it.id
                WHERE i.codigo_interferencia = ANY(%s)
                AND it.rotulo ~ '^[A-Z]{3}_[0-9]{3}$'
                LIMIT 1;
            """, (codigo_interferencia,))
            
            return cursor.fetchone()
        
        except Exception as e:
            print(f"Erro ao verificar interferência cadastrada: {e}")
            return None
        finally:
            if cursor:
                cursor.close()

    def atualizar_intervencao(self, id_intervencao):
        """Atualiza dados de uma intervenção existente.

        Args:
            id_intervencao (int): ID da intervenção a atualizar.

        Returns:
            tuple: (sucesso, rótulo) onde sucesso é bool.
        """
        try:
            cursor = self.conn.cursor()

            operadorid = self.id_operador

            modo_transmissao_id = self.obter_id_por_descricao(
                "tb_modo_transmissao", 
                self.modo_transmissao_combo.currentText()
            )

            tipo_medidor_id = self.obter_id_por_descricao(
                "tb_tipo_medidor",
                self.equipamento_combo.currentText()
            )

            codigo_uc_novo = self.codigo_medidor_input.text().strip() or None

            # 2. Validar valores numéricos
            vazao = float(self.vazao_input.text())
            potencia = int(self.potencia_input.text())

            # 3. Executar UPDATE na intervenção
            cursor.execute("""
                UPDATE tb_intervencao
                SET operador_telemetria = %s,
                    modo_transmissao_id = %s,
                    vazao_nominal = %s,
                    potencia = %s,
                    tipo_medidor_id = %s
                WHERE id = %s
                RETURNING rotulo;
            """, (operadorid, modo_transmissao_id, vazao, potencia, tipo_medidor_id, id_intervencao))

            rotulo = cursor.fetchone()[0]

            # 4. Atualizar ou inserir o codigo_uc da intervenção na nova tabela
            if codigo_uc_novo:
                cursor.execute("""
                    SELECT 1 FROM tb_codigo_uc_intervencao WHERE intervencao_id = %s
                """, (id_intervencao,))
                exists = cursor.fetchone()

                if exists:
                    cursor.execute("""
                        UPDATE tb_codigo_uc_intervencao
                        SET codigo_uc = %s
                        WHERE intervencao_id = %s
                    """, (codigo_uc_novo, id_intervencao))
                else:
                    cursor.execute("""
                        INSERT INTO tb_codigo_uc_intervencao (intervencao_id, codigo_uc)
                        VALUES (%s, %s)
                    """, (id_intervencao, codigo_uc_novo))

            self.conn.commit()
            return True, rotulo

        except Exception as e:
            self.conn.rollback()
            return False, str(e)
        finally:
            if cursor:
                cursor.close()

    def obter_id_por_descricao(self, tabela, descricao):
        """Obtém ID de registro a partir da descrição.
        
        Args:
            tabela (str): Nome da tabela.
            descricao (str): Descrição do registro.
            
        Returns:
            int: ID do registro ou None se não encontrado.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT id FROM {tabela} WHERE descricao = %s;", (descricao,))
            resultado = cursor.fetchone()
            return resultado[0] if resultado else None
        except Exception as e:
            print(f"Erro ao obter ID de {tabela}: {e}")
            return None
        finally:
            cursor.close()

    def obter_id_operador(self, tabela, nome):
        """Obtém ID de registro a partir do nome do operador.
        
        Args:
            tabela (str): Nome da tabela.
            nome (str): Nome do operador.
            
        Returns:
            int: ID do registro ou None se não encontrado.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT id FROM {tabela} WHERE nome = %s;", (nome,))
            resultado = cursor.fetchone()
            return resultado[0] if resultado else None
        except Exception as e:
            print(f"Erro ao obter ID de {tabela}: {e}")
            return None
        finally:
            cursor.close()

    def cadastrar_medidor(self):
        """Gerencia todo o fluxo de cadastro/atualização de medidores.
        
        Fluxo principal:
        1. Validação de interferências selecionadas
        2. Verificação de medidores existentes
        3. Confirmação com usuário
        4. Cadastro/atualização no banco
        5. Geração de relatório
        
        Trata ambos os casos:
        - Cadastro único (rótulo SIGLA_NNN)
        - Cadastro múltiplo (rótulos SIGLA_NNN_1, SIGLA_NNN_2, ...)
        
        Raises:
            Exception: Captura e trata qualquer erro no fluxo principal.
        """
        if getattr(self, '_em_atualizacao', False):
            return

        try:
            # Passo 1: Obter interferências selecionadas
            interferencias_selecionadas = self.obter_interferencias_selecionadas()
            if not interferencias_selecionadas:
                return

            # Passo 2: Verificar múltiplas interferências
            if len(interferencias_selecionadas) > 1 and not self.confirmar_multiplas_interferencias():
                return

            # Passo 3: Preparação de dados comuns
            sigla_base = self.obter_sigla_base(interferencias_selecionadas[0])
            numero_base = self.obter_numero_base(sigla_base)  # Número base para toda a sequência
            codigo_interferencia = interferencias_selecionadas

            # Passo 4: Verificação inicial para cadastro múltiplo
            if self.radio_nao.isChecked() and not getattr(self, '_em_cadastro_multiplo', False):
                resultado = self.verificar_medidores_antes_cadastro_multiplo(codigo_interferencia)
                if resultado is None:  # Caso especial quando escolheu "Atualizar"
                    return  # Já foi tratado no método acima
                elif not resultado:  # Escolheu "Cancelar"
                    return
                
                # Inicia cadastro múltiplo
                self._em_cadastro_multiplo = True
                self._dados_sequencia = {
                    'sigla_base': sigla_base,
                    'numero_base': numero_base,
                    'id_interferencia': None,
                    'medidores': []
                }

            # Passo 5: Verificar existência para cadastro único
            if self.radio_sim.isChecked():
                resultado = self.verificar_interferencia_cadastrada(codigo_interferencia)
                if resultado:
                    id_interferencia, id_intervencao, rotulo = resultado
                    self._em_atualizacao = True
                    try:
                        sucesso = self.tratar_medidor_existente(id_intervencao, codigo_interferencia)
                        return sucesso
                    finally:
                        self._em_atualizacao = False

            # Passo 6: Confirmar operação
            if not self.confirmar_operacao('cadastro', codigo_interferencia):
                return

            # Passo 7: Cadastro múltiplo em andamento
            if self.radio_nao.isChecked() and self._em_cadastro_multiplo:
                # Valida campos antes de cadastrar
                if not self.validar_campos_medidor():
                    return

                # Cadastra o medidor atual
                id_intervencao = self.cadastrar_intervencao(
                    codigo_interferencia,
                    sigla_base=self._dados_sequencia['sigla_base'],
                    numero_base=self._dados_sequencia['numero_base'],
                    medidor_num=self.medidor_atual
                )
                
                if not id_intervencao:
                    raise Exception(f"Falha ao cadastrar medidor {self.medidor_atual}")

                codigo_uc_novo = self.codigo_medidor_input.text().strip() or None
                self.cadastrar_codigo_uc_intervencao(id_intervencao, codigo_uc_novo)

                # Registra relação intervenção-interferência
                if not self._dados_sequencia['id_interferencia']:
                    self._dados_sequencia['id_interferencia'] = self.upsert_interferencia(codigo_interferencia)               
                self.cadastrar_intervencao_interferencia(id_intervencao, self._dados_sequencia['id_interferencia'])

                # Atualiza campo codigo_uc na tb_interferencia após todos os vínculos
                self.upsert_interferencia(codigo_interferencia)

                self._dados_sequencia['medidores'].append(id_intervencao)

                # Verifica se terminou a sequência
                if self.medidor_atual >= self.quantidade_medidores:
                    # Cria lista com todos os medidores cadastrados
                    medidores_relatorio = [{
                        'id_intervencao': med_id,
                        'numero': idx+1,
                        'codigo_interferencia': codigo_interferencia,
                        'rotulo': f"{self._dados_sequencia['sigla_base']}_{self._dados_sequencia['numero_base']:03d}_{idx+1:01d}"
                    } for idx, med_id in enumerate(self._dados_sequencia['medidores'])]
                    
                    self.gerenciar_fluxo_pos_cadastro(medidores_relatorio)
                    self.limpar_e_resetar()
                    self._em_cadastro_multiplo = False
                    del self._dados_sequencia
                else:
                    # Prepara próximo medidor
                    self.medidor_atual += 1
                    self.atualizar_interface_medidor()
                    self.limpar_campos_medidor()
                    
                    # MENSAGEM CRÍTICA ADICIONADA AQUI
                    QMessageBox.information(
                        self,
                        "Próximo Medidor",
                        f"Preencha os dados para o Medidor {self.medidor_atual} de {self.quantidade_medidores}"
                    )
                
                return

            # Passo 8: Cadastro único ou primeiro de múltiplos
            if self.radio_sim.isChecked():
                id_intervencao = self.cadastrar_intervencao(
                    codigo_interferencia,
                    sigla_base=sigla_base,
                    numero_base=numero_base
                )

                if not id_intervencao:
                    raise Exception("Falha ao cadastrar intervenção")

                # Vincula código_uc à intervenção
                codigo_uc_novo = self.codigo_medidor_input.text().strip() or None
                self.cadastrar_codigo_uc_intervencao(id_intervencao, codigo_uc_novo)

                # Realiza ou atualiza o vínculo com interferência
                id_interferencia = self.upsert_interferencia(
                    codigo_interferencia=codigo_interferencia,
                    nome_usuario=getattr(self, 'nome_usuario', 'Não informado'),
                    cnarh=getattr(self, 'cnarh_encontrado', 'Não informado')
                )

                if not id_interferencia:
                    raise Exception("Falha ao registrar interferência")

                self.cadastrar_intervencao_interferencia(id_intervencao, id_interferencia)

                # Atualiza campo codigo_uc na tb_interferencia após criar os vínculos
                self.upsert_interferencia(codigo_interferencia)

                self.gerenciar_fluxo_pos_cadastro([{
                    'id_intervencao': id_intervencao,
                    'numero': 1,
                    'codigo_interferencia': codigo_interferencia,
                    'rotulo': f"{sigla_base}_{numero_base:03d}"
                }])

                self.limpar_e_resetar()
                    
            # Início de cadastro múltiplo
            elif self.radio_nao.isChecked():
                self.quantidade_medidores = int(self.quantidade_medidor_combo.currentText())
                self.medidor_atual = 1
                self._em_cadastro_multiplo = True
                self._dados_sequencia = {
                    'sigla_base': sigla_base,
                    'numero_base': numero_base,
                    'id_interferencia': id_interferencia,
                    'medidores': []
                }
                self.atualizar_interface_medidor()
                
                # MENSAGEM PARA O PRIMEIRO MEDIDOR DA SEQUÊNCIA
                if self.quantidade_medidores > 1:
                    QMessageBox.information(
                        self,
                        "Início de Cadastro Múltiplo",
                        f"Preencha os dados para o Medidor 1 de {self.quantidade_medidores}"
                    )

        except Exception as e:
            # Limpeza de estado em caso de erro
            self._em_cadastro_multiplo = False
            if hasattr(self, '_dados_sequencia'):
                del self._dados_sequencia
            self.tratar_erro(e)
        finally:
            self._em_atualizacao = False

    def obter_sigla_base(self, codigo_interferencia):
        """Obtém sigla base para rótulo do medidor.
        
        Args:
            codigo_interferencia (str): Código da interferência.
            
        Returns:
            str: Sigla base ou 'DUG' se não encontrada.
        """
        cursor = self.conn.cursor()
        try:
            # Busca coordenadas da interferência
            query = """
            SELECT latitude, longitude
            FROM tb_mapserver_obrigatoriedade
            WHERE codigo_interferencia = %s;
            """
            cursor.execute(query, (codigo_interferencia,))
            resultado = cursor.fetchone()
            
            if not resultado:
                print("Nenhuma coordenada encontrada para a interferência selecionada.")
                return None
                
            lat, lon = resultado
            print(f"Coordenadas encontradas para a interferência {codigo_interferencia}: Latitude = {lat}, Longitude = {lon}")
            
            # Busca sigla na região
            cursor.execute("""
                SELECT sgautomonit
                FROM ft_unidade_automonitoramento
                WHERE ST_Intersects(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                LIMIT 1;
            """, (lon, lat))
            
            resultado_sigla = cursor.fetchone()
            if resultado_sigla:
                return resultado_sigla[0]
            else:
                print("Nenhuma sigla encontrada para as coordenadas fornecidas.")
                return 'DUG'
                
        finally:
            cursor.close()

    def obter_numero_base(self, sigla_base):
        """Obtém próximo número base disponível para a sigla.
        
        Args:
            sigla_base (str): Sigla base para consulta.
            
        Returns:
            int: Próximo número base disponível.
        """
        cursor = self.conn.cursor()
        try:
            # Primeiro encontra o número base mais alto para a sigla
            cursor.execute("""
            SELECT MAX(matches.codigo_int) AS max_codigo
            FROM 
                tb_intervencao,
                LATERAL (SELECT CAST((REGEXP_MATCHES(rotulo, '_([0-9]{3})'))[1] AS INTEGER) AS codigo_int) AS matches
            WHERE 
                rotulo LIKE %s;
            """, (f"{sigla_base}_%",))
            ultimo_numero = cursor.fetchone()[0] or 0
            return ultimo_numero + 1
        except Exception as e:
            print(f"Erro ao obter número base: {e}")
            return 1
        finally:
            cursor.close()

    def validar_campos_medidor(self):
        """Valida campos do medidor atual.
        
        Returns:
            bool: True se campos são válidos, False caso contrário.
        """
        campos_validos = (
            self.vazao_input.text().strip(),
            self.potencia_input.text().strip(),
            self.equipamento_combo.currentText(),
            self.modo_transmissao_combo.currentText()
        )
        
        if not all(campos_validos):
            QMessageBox.warning(
                self,
                "Campos Obrigatórios",
                "Preencha todos os campos do medidor atual antes de cadastrar"
            )
            return False
        return True
    
    def tratar_erro(self, exception):
        """Tratamento unificado de erros.
        
        Args:
            exception: Exceção capturada.
        """
        if self.radio_nao.isChecked():
            self.medidor_atual = max(1, self.medidor_atual - 1)
        
        error_msg = f"Falha no processo:\n{str(exception)}"
        
        # Log mais detalhado no console (opcional)
        import traceback
        print(f"ERRO: {error_msg}")
        traceback.print_exc()
        
        QMessageBox.critical(self, "Erro", error_msg)

    def confirmar_multiplas_interferencias(self):
        """Confirmação para múltiplas seleções de interferência.
        
        Returns:
            bool: True se usuário confirmou, False caso contrário.
        """
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Question)
        msg.setWindowTitle("Atenção: Múltiplas Seleções")
        msg.setText("Você selecionou múltiplas interferências.")
        msg.setInformativeText(
            "Atenção: Apenas a primeira interferência será usada para:\n"
            "- Coordenadas geográficas\n"
            "- Geoposicionamento\n"
            "- Localização na unidade de automonitoramento\n"
            "- Geração de ID rótulo do medidor\n\n"
            "Deseja continuar mesmo assim?"
        )
        msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
        msg.setDefaultButton(QMessageBox.No)
        return msg.exec_() == QMessageBox.Yes

    def obter_outorga_selecionada(self):
        """Obtém todas as outorgas selecionadas no tree widget separadas por vírgula.
        
        Returns:
            str: String com todas as outorgas selecionadas separadas por vírgula 
                 ou None se nenhuma estiver selecionada.
        """
        outorgas_selecionadas = []
        
        for i in range(self.outorga_tree.topLevelItemCount()):
            item = self.outorga_tree.topLevelItem(i)
            checkbox = self.outorga_tree.itemWidget(item, 5)
            if checkbox and checkbox.isChecked():
                outorgas_selecionadas.append(item.text(0))
        
        if not outorgas_selecionadas:
            return None
        
        # Retorna todas as outorgas separadas por vírgula
        return ', '.join(outorgas_selecionadas)

    def obter_interferencias_selecionadas(self):
        """
        Obtém códigos de interferências selecionadas no tree widget.
        
        Este método retorna uma lista com os códigos de todas as interferências
        selecionadas no tree widget.
        
        Returns:
            list: Lista de códigos de interferência selecionadas.
        """
        interferencias = []
        for i in range(self.tree_widget.topLevelItemCount()):
            item = self.tree_widget.topLevelItem(i)
            checkbox = self.tree_widget.itemWidget(item, 1)
            if checkbox and checkbox.isChecked():
                interferencias.append(item.text(0))
        
        if not interferencias:
            QMessageBox.warning(self, "Nenhuma Interferência", "Selecione pelo menos uma interferência.")
            return []
        
        return interferencias

    def obter_medidores_existentes(self, codigo_interferencia):
        """Obtém medidores existentes para uma interferência.
        
        Args:
            codigo_interferencia (str): Código da interferência.
            
        Returns:
            dict: Dicionário com medidores organizados por tipo.
        """
        cursor = self.conn.cursor()
        try:
            # Garantir que codigo_interferencia seja uma lista
            if isinstance(codigo_interferencia, str):
                codigo_interferencia = [codigo_interferencia]
                
            # Busca todos os medidores da interferência, com tipo (ÚNICO/MÚLTIPLO)
            placeholders = ', '.join(['%s'] * len(codigo_interferencia))
            
            query = f"""
                SELECT 
                    it.rotulo,
                    CASE 
                        WHEN it.rotulo ~ '^[A-Z]{{3}}_[0-9]{{3}}$' THEN 'ÚNICO'
                        WHEN it.rotulo ~ '^[A-Z]{{3}}_[0-9]{{3}}_[0-9]+$' THEN 'MÚLTIPLO'
                        ELSE 'OUTRO'
                    END as tipo
                FROM tb_interferencia i
                JOIN tb_intervencao_interferencia ii ON i.id = ii.interferencia_id
                JOIN tb_intervencao it ON ii.intervencao_id = it.id
                WHERE i.codigo_interferencia IN ({placeholders})
                ORDER BY it.rotulo;
            """
            
            cursor.execute(query, codigo_interferencia)
            
            resultados = cursor.fetchall()
            
            # Organiza em dicionário
            return {
                'unico': next((r[0] for r in resultados if r[1] == 'ÚNICO'), None),
                'multiplos': [r[0] for r in resultados if r[1] == 'MÚLTIPLO'],
                'total': len(resultados)
            }
        finally:
            cursor.close()

    def verificar_medidores_antes_cadastro_multiplo(self, codigo_interferencia):
        """Verifica medidores existentes antes de cadastro múltiplo.
        
        Args:
            codigo_interferencia (str): Código da interferência.
            
        Returns:
            bool: True se pode prosseguir, False caso contrário.
        """
        if getattr(self, '_em_cadastro_multiplo', False):
            return True
                
        medidores = self.obter_medidores_existentes(codigo_interferencia)
        qtd_novos = int(self.quantidade_medidor_combo.currentText())
        
        # Se não há medidores existentes, pode prosseguir
        if not medidores['total']:
            return True
        
        # Se há medidores múltiplos mas não há único
        if not medidores['unico'] and medidores['multiplos']:
            # Construção da mensagem personalizada para múltiplos existentes
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Question)
            msg.setWindowTitle("Medidores Múltiplos Existentes")
            
            texto = f"Interferência {codigo_interferencia} já possui {len(medidores['multiplos'])} medidores múltiplos:\n"
            for rotulo in medidores['multiplos']:
                texto += f"\n• {rotulo}"
            
            msg.setText(texto)
            msg.setInformativeText("Deseja atualizar os dados de um medidor existente, cadastrar mais medidores ou cancelar a operação?")
            
            # Adiciona os botões personalizados
            btn_atualizar = msg.addButton("Atualizar", QMessageBox.ActionRole)
            btn_cadastrar = msg.addButton("Cadastrar", QMessageBox.ActionRole)
            btn_cancelar = msg.addButton("Cancelar", QMessageBox.RejectRole)
            
            msg.setDefaultButton(btn_cancelar)
            msg.exec_()
            
            # Trata a resposta
            if msg.clickedButton() == btn_atualizar:
                return self.tratar_atualizacao_multipla(codigo_interferencia, medidores['multiplos'])
            elif msg.clickedButton() == btn_cadastrar:
                return True
            else:  # Cancelar
                return False
            
        # Caso especial: só existe medidor único
        if medidores['unico'] and not medidores['multiplos']:
            resposta = QMessageBox.question(
                self,
                "Medidor Único Existente",
                f"A interferência {codigo_interferencia} já possui um medidor único:\n\n"
                f"• {medidores['unico']}\n\n"
                "Caso queira atualizar os dados deste medidor, fecha esta janela e selecione a opção 'Sim' no campo "
                "'Possui apenas um medidor para uma ou mais interferência/outorga?'.\n\n"
                "Caso contrário, clique no botão 'Sim' abaixo e continue com o cadastro dos medidores.",
                QMessageBox.Yes | QMessageBox.No
            )
            return resposta == QMessageBox.Yes
        
        # Caso com ambos único e múltiplos
        if medidores['unico'] and medidores['multiplos']:
            # Construção da mensagem personalizada
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Question)
            msg.setWindowTitle("Medidores Existentes")
            
            texto = f"Interferência {codigo_interferencia} já possui:\n"
            texto += f"• 1 medidor único: {medidores['unico']}\n"
            texto += f"• {len(medidores['multiplos'])} medidores múltiplos:\n"
            for rotulo in medidores['multiplos']:
                texto += f"  - {rotulo}\n"
            
            msg.setText(texto)
            msg.setInformativeText("Deseja atualizar os dados de um medidor, cadastrar mais medidores ou cancelar a operação?")
            
            # Adiciona os botões personalizados
            btn_atualizar = msg.addButton("Atualizar", QMessageBox.ActionRole)
            btn_cadastrar = msg.addButton("Cadastrar", QMessageBox.ActionRole)
            btn_cancelar = msg.addButton("Cancelar", QMessageBox.RejectRole)
            
            # Desabilita o botão Atualizar se não houver medidores múltiplos
            if not medidores['multiplos']:
                btn_atualizar.setEnabled(False)
                btn_atualizar.setToolTip("Não há medidores múltiplos para atualizar")
            
            msg.setDefaultButton(btn_cancelar)
            msg.exec_()
            
            # Trata a resposta
            if msg.clickedButton() == btn_atualizar:
                return self.tratar_atualizacao_multipla(codigo_interferencia, medidores['multiplos'])
            elif msg.clickedButton() == btn_cadastrar:
                return True
            else:  # Cancelar
                return False
        
        return True
            
    def tratar_medidor_existente(self, id_intervencao, codigo_interferencia):
        """Gerencia atualização de medidor já cadastrado.
        
        Args:
            id_intervencao (int): ID da intervenção existente.
            codigo_interferencia (str): Código INT_CD para referência.
            
        Returns:
            bool: True se atualização foi concluída com sucesso.
            
        Fluxo:
            1. Obtém dados atuais do medidor
            2. Exibe diálogo de confirmação
            3. Executa atualização
            4. Exibe relatório
        """
        try:
            # 1. Obter dados atuais
            dados = self.obter_dados_intervencao(id_intervencao)
            if not dados:
                QMessageBox.warning(self, "Aviso", "Dados do medidor existente não encontrados.")
                return False

            # 2. Diálogo de confirmação
            resposta = QMessageBox.question(
                self,
                "Medidor Existente",
                self.gerar_mensagem_medidor_existente(codigo_interferencia, dados),
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )

            if resposta != QMessageBox.Yes:
                return False

            # 3. Processar atualização
            sucesso = self.processar_atualizacao(id_intervencao)
            if sucesso:
                self.limpar_e_resetar()
            return sucesso

        except Exception as e:
            self.tratar_erro(e)
            return False

    def tratar_atualizacao_multipla(self, codigo_interferencia, medidores):
        """Gerencia atualização de medidores múltiplos existentes.
        
        Args:
            codigo_interferencia (str): Código da interferência.
            medidores (list): Lista de medidores existentes.
            
        Returns:
            bool: True se atualização foi bem sucedida.
        """
        # Diálogo para selecionar qual medidor atualizar
        dialog = QDialog(self)
        dialog.setWindowTitle("Selecionar Medidor para Atualizar")
        dialog.setFixedSize(400, 300)
        
        layout = QVBoxLayout()
        label = QLabel("Selecione o medidor que deseja atualizar:")
        layout.addWidget(label)
        
        list_widget = QListWidget()
        for medidor in medidores:
            list_widget.addItem(medidor)
        layout.addWidget(list_widget)
        
        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.accepted.connect(dialog.accept)
        btn_box.rejected.connect(dialog.reject)
        layout.addWidget(btn_box)
        
        dialog.setLayout(layout)
        
        if dialog.exec_() != QDialog.Accepted or not list_widget.currentItem():
            return False
        
        medidor_selecionado = list_widget.currentItem().text()
        
        # Busca o ID da intervenção correspondente ao rótulo selecionado
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT i.id 
                FROM tb_intervencao i
                JOIN tb_intervencao_interferencia ii ON i.id = ii.intervencao_id
                JOIN tb_interferencia inf ON ii.interferencia_id = inf.id
                WHERE i.rotulo = %s AND inf.codigo_interferencia = ANY(%s)
            """, (medidor_selecionado, codigo_interferencia))
            
            resultado = cursor.fetchone()
            if not resultado:
                QMessageBox.warning(self, "Aviso", "Medidor selecionado não encontrado no banco de dados.")
                return False
                
            id_intervencao = resultado[0]
            
            # Usa o método existente de atualização de medidor único
            sucesso = self.processar_atualizacao(id_intervencao)
            return False  # Retorna False para interromper o fluxo de cadastro múltiplo
            
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Falha ao buscar medidor: {str(e)}")
            return False
        finally:
            cursor.close()

    def gerar_mensagem_medidor_existente(self, codigo_interferencia, dados):
        """Gera mensagem detalhada sobre medidor existente.
        
        Args:
            codigo_interferencia (str): Código da interferência.
            dados (dict): Dados do medidor existente.
            
        Returns:
            str: Mensagem formatada.
        """
        return (f"ATENÇÃO! A interferência {codigo_interferencia} já possui medidor:\n\n"
                f"ID: {dados['rotulo']}\n"
                f"Relógio: {dados['codigo_uc'] or 'Não informado'}\n"
                f"Vazão: {dados['vazao_nominal']} m³/s\n"
                f"Potência: {dados['potencia']} kW\n"
                f"Equipamento: {dados['tipo_medidor']}\n"
                f"Transmissão: {dados['modo_transmissao']}\n\n"
                "Deseja atualizar este medidor com os novos dados?")

    def gerenciar_fluxo_pos_cadastro(self, medidores_cadastrados=None):
        """Coordena ações após cadastro bem-sucedido.
        
        Args:
            medidores_cadastrados (list, optional): Lista de dicts com:
                - id_intervencao (int)
                - numero (int)
                - codigo_interferencia (str)
                - rotulo (str)
                
        Actions:
            1. Gera relatório HTML com dados cadastrados
            2. Exibe diálogo com resumo
            3. Atualiza notificação visual
            4. Prepara interface para próxima operação
            5. Carrega medidor(es) no projeto QGIS caso não esteja carregada
        """
        try:
            # 1. Preparação da estrutura de relatório
            outorga_selecionada = self.obter_outorga_selecionada() or 'Não informado'
            
            relatorio = {
                'operacao': 'cadastro',
                'dados_base': {
                    'nome_usuario': self.nome_usuario_input.text() if hasattr(self, 'nome_usuario_input') else 'Não informado',
                    'operador': self.operador_combo.currentText() if hasattr(self, 'operador_combo') else 'Não informado',
                    'cnarh': self.cnarh_input.text() if hasattr(self, 'cnarh_input') else 'Não informado',
                    'outorga': outorga_selecionada,
                    'codigo_interferencia': ''
                },
                'medidores': []
            }

            # 2. Montar lista de medidores a partir do parâmetro ou dados em memória
            if medidores_cadastrados is not None:
                lista_medidores = medidores_cadastrados if isinstance(medidores_cadastrados, (list, tuple)) else [medidores_cadastrados]
            elif hasattr(self, '_dados_sequencia') and hasattr(self._dados_sequencia, 'get'):
                lista_medidores = [{'id_intervencao': med_id, 'numero': idx + 1}
                                   for idx, med_id in enumerate(self._dados_sequencia.get('medidores', []))]
            else:
                lista_medidores = []
                
            # 3. Consulta para cada medidor
            medidores_ids = []
            for medidor in lista_medidores:
                medidor_id = medidor.get('id_intervencao')
                if not medidor_id:
                    continue

                medidores_ids.append(medidor_id)

                try:                    
                    # Consulta os dados completos do medidor
                    with self.conn.cursor() as cursor:
                        cursor.execute("""
                            SELECT rotulo_medidor, vazao_nominal, potencia,
                                   tipo_medidor, modo_transmissao,
                                   nu_interferencia_cnarh, codigo_uc
                            FROM view_ft_intervencao
                            WHERE id = %s
                        """, (medidor_id,))
                        
                        dados = cursor.fetchone()
                        if dados:
                            relatorio['medidores'].append({
                                'numero': medidor.get('numero', len(relatorio['medidores']) + 1),
                                'rotulo': dados[0],
                                'codigo_uc': dados[6] or 'Não informado',                                
                                'vazao': dados[1],
                                'potencia': dados[2],
                                'equipamento': dados[3],
                                'transmissao': dados[4]
                            })
                            
                            if not relatorio['dados_base']['codigo_interferencia']:
                                relatorio['dados_base']['codigo_interferencia'] = dados[5]

                except Exception as e:
                    print(f"Erro ao processar medidor {medidor_id}: {str(e)}")

            # 4. Exibição do relatório
            if relatorio['medidores']:
                self.exibir_resultado_consolidado(relatorio)
                if medidores_ids and iface is not None:
                    self.exibir_medidores_no_canvas(medidores_ids)
                    self.showMinimized()                    
            else:
                QMessageBox.warning(self, "Aviso", "Nenhum dado de medidor foi encontrado para gerar o relatório.")

        except Exception as e:
            QMessageBox.critical(
                self,
                "Erro no Relatório",
                f"Falha ao gerar relatório:\n{str(e)}\n\n"
                f"Tipo do erro: {type(e).__name__}"
            )

    def exibir_resultado_consolidado(self, relatorio):
        """Exibe relatório consolidado de operação.
        
        Args:
            relatorio (dict): Dicionário com dados do relatório.
        """
        try:
            # 1. Preparação do HTML
            html = f"""
            <html>
            <body style="font-family: Arial; margin: 20px;">
                <h2 style="color: #2c5aa0;">Relatório de {relatorio['operacao'].title()}</h2>
                <div style="margin-bottom: 20px; border-bottom: 1px solid #eee; padding-bottom: 10px;">
                    <p><strong>Usuário:</strong> {relatorio['dados_base']['nome_usuario']}</p>
                    <p><strong>Operador:</strong> {relatorio['dados_base']['operador']}</p>
                    <p><strong>CNARH:</strong> {relatorio['dados_base']['cnarh']}</p>
                    <p><strong>Resolução(ões) ANA:</strong> {relatorio['dados_base']['outorga']}</p>
                    <p><strong>Interferência(s):</strong> {relatorio['dados_base']['codigo_interferencia']}</p>
                </div>
            """

            # 2. Adição de cada medidor
            for medidor in relatorio['medidores']:
                html += f"""
                <div style="background: #f8f9fa; padding: 15px; margin-bottom: 15px; border-radius: 5px;">
                    <h3 style="color: #2c5aa0;">Medidor {medidor.get('numero', '-')}: {medidor.get('rotulo', '-')}</h3>
                    <p><strong>Código UC:</strong> {medidor.get('codigo_uc', '-')}</p>
                    <p><strong>Vazão:</strong> {medidor.get('vazao', '-')} m³/s</p>
                    <p><strong>Potência:</strong> {medidor.get('potencia', '-')} kW</p>
                    <p><strong>Equipamento:</strong> {medidor.get('equipamento', '-')}</p>
                    <p><strong>Transmissão:</strong> {medidor.get('transmissao', '-')}</p>
                    <p style="color: #28a745;"><strong>Status:</strong> {relatorio['operacao'].upper()}</p>
                </div>
                """

            html += "</body></html>"

            # 3. Configuração da janela
            dialog = QDialog(self)
            dialog.setWindowTitle("Relatório Completo")
            dialog.resize(700, 800)
            
            text_edit = QTextEdit()
            text_edit.setHtml(html)
            text_edit.setReadOnly(True)

            btn_copiar = QPushButton("Copiar para e-mail")
            btn_copiar.setStyleSheet("""
                background-color: #2050b8;
                color: white;
                font-size: 12px;
                font-weight: bold;
                padding: 10px;
            """)
            btn_copiar.clicked.connect(lambda: self.copiar_texto_relatorio(relatorio))
            
            btn_fechar = QPushButton("Fechar")
            btn_fechar.setStyleSheet("""
                background-color: #5474b8;
                color: white;
                font-size: 12px;
                font-weight: bold;
                padding: 10px;
            """)            
            btn_fechar.clicked.connect(dialog.accept)
            
            btn_layout = QHBoxLayout()
            btn_layout.addWidget(btn_copiar)
            btn_layout.addWidget(btn_fechar)
            btn_layout.setSpacing(10)  # Espaço entre os botões
            
            layout = QVBoxLayout()
            layout.addWidget(text_edit)
            layout.addLayout(btn_layout)
            dialog.setLayout(layout)
            
            dialog.exec_()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Erro de Exibição",
                f"Não foi possível exibir o relatório:\n{str(e)}"
            )

    def copiar_texto_relatorio(self, relatorio):
        """Copia o relatório para a área de transferência, convertendo *texto* em negrito Unicode (UTF-8)."""

        def to_bold_unicode(texto: str) -> str:
            """Converte texto normal para caracteres em negrito Unicode (UTF-8)."""
            bold_map = {}

            # A–Z
            for i, c in enumerate(range(ord("A"), ord("Z") + 1)):
                bold_map[chr(c)] = chr(0x1D400 + i)
            # a–z
            for i, c in enumerate(range(ord("a"), ord("z") + 1)):
                bold_map[chr(c)] = chr(0x1D41A + i)
            # 0–9
            for i, c in enumerate(range(ord("0"), ord("9") + 1)):
                bold_map[chr(c)] = chr(0x1D7CE + i)

            return "".join(bold_map.get(ch, ch) for ch in texto)

        try:
            # Montar o texto com *asteriscos*
            texto = "Senhor(a),\n"
            texto += "Informamos que o seu cadastro de medidores de uso da água foi realizado no sistema Telemetria da ANA, conforme dados abaixo:\n\n"

            texto += f"Usuário: *{relatorio['dados_base']['nome_usuario']}*\n"

            operador_texto = relatorio['dados_base']['operador']
            if "(" in operador_texto and ")" in operador_texto:
                operador_nome = operador_texto.split("(")[0].strip()
                operador_email = operador_texto.split("(")[1].replace(")", "").strip()
                texto += f"Operador: *{operador_nome}* ({operador_email})\n"
            else:
                texto += f"Operador: *{operador_texto}*\n"

            texto += f"CNARH: {relatorio['dados_base']['cnarh']}\n"
            texto += f"Outorga(s): {relatorio['dados_base']['outorga']}\n"
            texto += f"Interferência(s): {relatorio['dados_base']['codigo_interferencia']}\n\n"

            texto += "*Rótulo para uso na API dos medidores:*\n"

            for medidor in relatorio['medidores']:
                texto += f"Medidor {medidor.get('numero', '-')}: *{medidor.get('rotulo', '-')}*\n"
                texto += f"Código UC: {medidor.get('codigo_uc', 'Não informado')}\n"
                texto += f"Vazão: {medidor.get('vazao', '-')} m³/s\n"
                texto += f"Potência: {medidor.get('potencia', '-')} kW\n"
                texto += f"Equipamento: {medidor.get('equipamento', '-')}\n"
                texto += f"Transmissão: {medidor.get('transmissao', '-')}\n\n"

            texto += "Atenciosamente,\n"

            # Converter *trechos* para negrito Unicode
            def bold_replacer(match):
                return to_bold_unicode(match.group(1))

            texto_formatado = re.sub(r"\*(.*?)\*", bold_replacer, texto)

            # Copiar para área de transferência
            mime_data = QMimeData()
            mime_data.setText(texto_formatado)
            clipboard = QApplication.clipboard()
            clipboard.setMimeData(mime_data)

            # Mensagem de confirmação
            QMessageBox.information(
                self.parent() if hasattr(self, 'parent') else None,
                "Texto Copiado",
                "O relatório foi copiado para a área de transferência!"
            )

        except Exception as e:
            QMessageBox.warning(
                self.parent() if hasattr(self, 'parent') else None,
                "Erro ao Copiar",
                f"Não foi possível copiar o texto:\n{str(e)}"
            )

    def adicionar_openstreetmap(self):
        """Adiciona o OpenStreetMap como alternativa de mapa base."""
        try:
            # Configuração do provedor XYZ
            url_with_params = (
                "type=xyz&url=https://tile.openstreetmap.org/{z}/{x}/{y}.png"
                "&zmin=0&zmax=19"
                "&crs=EPSG:3857"
            )
            
            # Criar a camada raster
            osm_layer = QgsRasterLayer(url_with_params, "OpenStreetMap", "wms")
            
            if not osm_layer.isValid():
                QMessageBox.warning(None, "Erro", "Falha ao carregar OpenStreetMap. Verifique sua conexão com a internet.")
                return
            
            # Adicionar ao projeto
            QgsProject.instance().addMapLayer(osm_layer)
            
        except Exception as e:
            QMessageBox.critical(None, "Erro", f"Erro ao carregar OpenStreetMap: {str(e)}")

    def adicionar_google_satellite(self):
        """Adiciona camada de satélite da Google."""
        try:
            layers = QgsProject.instance().mapLayersByName("Google Satellite Hybrid")
            if layers:
                iface.mapCanvas().setCurrentLayer(layers[0])
                return

            uri = "type=xyz&url=https://mt1.google.com/vt/lyrs%3Dy%26x%3D%7Bx%7D%26y%3D%7By%7D%26z%3D%7Bz%7D"
            rlayer = QgsRasterLayer(uri, "Google Satellite Hybrid", "wms")
            if rlayer.isValid():
                QgsProject.instance().addMapLayer(rlayer)
        except Exception as e:
            print(f"Erro ao adicionar Google: {e}")
    
    def adicionar_obrigatorias(self):
        """Carrega as captações obrigatórias a partir da view 'public.view_ft_captacao_obrigatoriedade'
        e aplica estilo categorizado pelo campo 'cadastrado' (sim/não).
        
        Esta função realiza as seguintes operações:
            1. Define explicitamente os parâmetros de conexão ao banco PostgreSQL
            2. Cria a URI de conexão usando QgsDataSourceUri
            3. Configura a fonte de dados apontando para a view 'public.view_ft_captacao_obrigatoriedade'
            4. Aplica filtro para exibir na legenda
            5. Configura simbologia e legenda

        Retorno:
            QgsVectorLayer: Camada carregada no QGIS ou None em caso de erro.        
        """
        try:
            # === RECUPERAÇÃO DE PARÂMETROS (alterado para seguir padrão da adicionar_intervencoes) ===
            dsn_params = self.conn.get_dsn_parameters()
            host = dsn_params.get('host')
            dbname = dsn_params.get('dbname', 'telemetria')
            user = dsn_params.get('user', self.usuario_logado)
            port = dsn_params.get('port', '5432')
            
            # Usa a senha que foi passada no construtor
            password = self.senha_conexao

            # Validação de segurança
            if not password:
                QMessageBox.critical(
                    self, "Erro de Autenticação", 
                    "A senha não foi transmitida. Verifique se a janela foi aberta corretamente pelo Login."
                )
                return None

            print(f"🔌 [Carregar Captações Obrigatórias] Conectando como: {user}")

            # 2. Configurar a conexão URI (agora usando setConnection)
            uri = QgsDataSourceUri()
            uri.setConnection(host, port, dbname, user, password)
            
            # 3. Configurar a fonte de dados
            uri.setDataSource(
                'public',                    # schema
                'view_ft_captacao_obrigatoriedade',  # table
                'geom',                      # geometry column
                "",                          # filter (vazio para nenhum filtro)
                "id"                          # key column
            )

            # 4. Carregar a camada diretamente do banco
            layer = QgsVectorLayer(uri.uri(), "Captação obrigada ao cadastro", "postgres")

            if not layer.isValid():
                error_msg = (
                    "Falha ao carregar view do banco de dados!\n"
                    f"Verifique:\n"
                    f"- Conexão com {host}:{port}\n"
                    f"- Acesso ao banco {dbname}\n"
                    f"- Existência da view public.view_ft_captacao_obrigatoriedade\n"
                    f"- Nome da coluna geométrica: 'geom'"
                )
                raise Exception(error_msg)

            # 5. Verificar se o campo 'cadastrado' existe
            if 'cadastrado' in [field.name() for field in layer.fields()]:
                categories = [
                    QgsRendererCategory('sim', 
                        QgsMarkerSymbol.createSimple({
                            'name': 'diamond',
                            'color': '#4CAF50',  # Verde
                            'size': '4.0',
                            'outline_color': 'black',
                            'outline_width': '0.4'
                        }), 
                        'Cadastrada'),
                    QgsRendererCategory('não', 
                        QgsMarkerSymbol.createSimple({
                            'name': 'diamond',
                            'color': '#F44336',  # Vermelho
                            'size': '4.0',
                            'outline_color': 'black',
                            'outline_width': '0.4'
                        }), 
                        'Não cadastrada')
                ]
                layer.setRenderer(QgsCategorizedSymbolRenderer('cadastrado', categories))
            else:
                # Estilo padrão se o campo não existir
                layer.renderer().setSymbol(QgsMarkerSymbol.createSimple({
                    'name': 'circle',
                    'color': 'yellow',
                    'size': '3.0',
                    'outline_color': 'black',
                    'outline_width': '0.3'
                }))

            # 6. Adicionar ao projeto
            QgsProject.instance().addMapLayer(layer)

            # 7. Retornar a layer criada para possível uso posterior
            return layer

        except Exception as e:
            QMessageBox.critical(
                self,
                "Erro ao carregar captações obrigatórias",
                f"Erro detalhado:\n{str(e)}\n\n"
                f"Parâmetros usados:\n"
                f"Host: {host if 'host' in locals() else 'n/d'}\n"
                f"Banco: {dbname if 'dbname' in locals() else 'n/d'}\n"
                f"View: public.view_ft_captacao_obrigatoriedade"
            )
            return None
            
    def adicionar_intervencoes(self, id_intervencao):
        """
        Carrega e exibe no QGIS as intervenções filtradas a partir da view 'public.view_ft_intervencao'.

        Esta função realiza as seguintes operações:
        1. Define explicitamente os parâmetros de conexão ao banco PostgreSQL
        2. Cria a URI de conexão usando QgsDataSourceUri
        3. Configura a fonte de dados apontando para a view 'public.view_ft_intervencao'
        4. Aplica filtro para exibir apenas as intervenções com IDs especificados
        5. Configura simbologia e rótulos da camada
        6. Adiciona a camada ao projeto QGIS no topo da legenda

        Parâmetros:
            id_intervencao (int ou list): ID(s) da(s) intervenção(ões) que devem ser exibidas.

        Retorno:
            QgsVectorLayer: Camada carregada no QGIS ou None em caso de erro.
        """
        try:
            # 1. Garantir lista de IDs
            ids = [id_intervencao] if not isinstance(id_intervencao, list) else id_intervencao
            if not ids:
                QMessageBox.warning(self, "Aviso", "Nenhum ID de intervenção fornecido.")
                return None

            # === RECUPERAÇÃO DE PARÂMETROS (alterado para seguir o padrão) ===
            dsn_params = self.conn.get_dsn_parameters()
            host = dsn_params.get('host')
            dbname = dsn_params.get('dbname', 'telemetria')
            user = dsn_params.get('user', self.usuario_logado)
            port = dsn_params.get('port', '5432')
            
            # Usa a senha que foi passada no construtor
            password = self.senha_conexao

            # Validação de segurança
            if not password:
                QMessageBox.critical(
                    self, "Erro de Autenticação", 
                    "A senha não foi transmitida. Verifique se a janela foi aberta corretamente pelo Login."
                )
                return None

            print(f"🔌 [Carregar Intervenção] Conectando como: {user}")

            # 3. Configurar conexão URI (agora usando setConnection com parâmetros diretos)
            uri = QgsDataSourceUri()
            uri.setConnection(host, port, dbname, user, password)

            # 4. Fonte de dados
            uri.setDataSource(
                'public',                    # schema
                'view_ft_intervencao',        # table
                'geom',                       # geometry column
                "",                           # filter (vazio para nenhum filtro)
                "id"                           # key column
            )

            # 5. Carregar camada
            layer = QgsVectorLayer(uri.uri(), "Medidor recém cadastrado", "postgres")
            if not layer.isValid():
                error_msg = (
                    "Falha ao carregar view do banco de dados!\n"
                    f"Verifique:\n"
                    f"- Conexão com {host}:{port}\n"
                    f"- Acesso ao banco {dbname}\n"
                    f"- Existência da view public.view_ft_intervencao\n"
                    f"- Nome da coluna geométrica: 'geom'"
                )
                QMessageBox.critical(self, "Erro", error_msg)
                return None

            # 6. Filtrar por IDs
            id_list_str = ",".join(str(i) for i in ids)
            layer.setSubsetString(f"id IN ({id_list_str})")
            print(f"✅ Camada carregada com {layer.featureCount()} feições para IDs: {id_list_str}")

            # 7. Configuração visual
            symbol = QgsMarkerSymbol.createSimple({
                'name': 'circle',
                'color': 'yellow',
                'size': '5.0',
                'outline_color': 'black',
                'outline_width': '0.4'
            })
            layer.renderer().setSymbol(symbol)

            # 8. Rótulos
            settings = QgsPalLayerSettings()
            settings.fieldName = 'rotulo_medidor'
            settings.isExpression = False

            if hasattr(settings, 'dataDefinedProperties'):
                from qgis.core import QgsProperty
                settings.placement = QgsPalLayerSettings.AroundPoint
                settings.dist = 5
                settings.distUnits = QgsUnitTypes.RenderMillimeters
                settings.dataDefinedProperties().setProperty(
                    QgsPalLayerSettings.LabelDistance,
                    QgsProperty.fromExpression("CASE WHEN @map_scale < 50000 THEN 5 ELSE 3 END")
                )
            else:
                settings.placement = QgsPalLayerSettings.AroundPoint
                settings.dist = 5
                settings.distUnits = QgsUnitTypes.RenderMillimeters

            text_format = QgsTextFormat()
            text_format.setSize(10)
            text_format.setColor(QColor(0, 0, 0))
            buffer = text_format.buffer()
            buffer.setEnabled(True)
            buffer.setSize(0.5)
            buffer.setColor(QColor(255, 255, 255))

            settings.setFormat(text_format)
            layer.setLabelsEnabled(True)
            layer.setLabeling(QgsVectorLayerSimpleLabeling(settings))

            # 9. Adicionar ao projeto
            QgsProject.instance().addMapLayer(layer, False)
            root = QgsProject.instance().layerTreeRoot()
            root.insertChildNode(0, QgsLayerTreeLayer(layer))

            return layer

        except Exception as e:
            import traceback
            print(f"❌ ERRO DETALHADO:\n{traceback.format_exc()}")
            QMessageBox.critical(self, "Erro Fatal", f"Erro inesperado: {str(e)}")
            return None
            
    def exibir_medidores_no_canvas(self, id_intervencao):
        """
        Exibe no QGIS as intervenções (medidores) recém-cadastradas, centralizando e ajustando o zoom.

        Operações:
        1. Adiciona camadas base (ESRI, OSM e obrigatórias)
        2. Usa adicionar_intervencoes() para carregar a camada filtrada por ID(s)
        3. Ajusta o extent e o zoom do mapa

        Parâmetros:
            id_intervencao (int ou list): ID(s) da(s) intervenção(ões) a exibir.

        Retorno:
            None
        """
        try:
            if iface is None:
                QMessageBox.warning(self, "Aviso", "Funcionalidade disponível apenas no QGIS")
                return

            project = QgsProject.instance()
            canvas = iface.mapCanvas()

            # 1. Adicionar camadas base
            self.adicionar_google_satellite()
            self.adicionar_openstreetmap()

            # 2. Adicionar camada de intervenções (medidores)
            medidores_layer = self.adicionar_intervencoes(id_intervencao)
            if not medidores_layer:
                return

            # 3. Ajustar extensão e zoom
            extent = medidores_layer.extent()
            features = list(medidores_layer.getFeatures())

            if extent.isEmpty() or extent.width() == 0 or extent.height() == 0:
                if not features:
                    QMessageBox.warning(self, "Aviso", "Nenhuma geometria válida encontrada")
                    return
                first_geom = features[0].geometry()
                if first_geom and first_geom.isMultipart():
                    ponto = first_geom.asMultiPoint()[0]
                else:
                    ponto = first_geom.asPoint()
                buffer = 0.0005
                extent = QgsRectangle(
                    ponto.x() - buffer, ponto.y() - buffer,
                    ponto.x() + buffer, ponto.y() + buffer
                )

            if medidores_layer.crs() != canvas.mapSettings().destinationCrs():
                transform = QgsCoordinateTransform(
                    medidores_layer.crs(),
                    canvas.mapSettings().destinationCrs(),
                    project
                )
                extent = transform.transformBoundingBox(extent)

            if extent.width() > 0 and extent.height() > 0:
                extent.scale(1.1)

            canvas.setRenderFlag(False)
            canvas.setExtent(extent)
            canvas.setRenderFlag(True)
            QTimer.singleShot(100, lambda: canvas.zoomScale(200000))
            canvas.refresh()

            QMessageBox.information(self, "Sucesso", f"{len(features)} medidor(es) exibido(s) no mapa!")

            # 4. Adicionar captações obrigadas ao cadastro
            self.adicionar_obrigatorias()

        except Exception as e:
            QMessageBox.critical(self, "Erro Fatal", f"Erro inesperado: {str(e)}")
            import traceback
            traceback.print_exc()

    def obter_dados_intervencao(self, id_intervencao):
        """Obtém dados de uma intervenção existente.
        
        Args:
            id_intervencao (int): ID da intervenção.
            
        Returns:
            dict: Dados da intervenção ou None se não encontrado.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT i.rotulo, i.vazao_nominal, i.potencia,
                       tm.descricao, mt.descricao,
                       COALESCE(inf.codigo_uc, 'Não informado') as codigo_uc
                FROM tb_intervencao i
                JOIN tb_tipo_medidor tm ON i.tipo_medidor_id = tm.id
                JOIN tb_modo_transmissao mt ON i.modo_transmissao_id = mt.id
                JOIN tb_intervencao_interferencia ii ON i.id = ii.intervencao_id
                JOIN tb_interferencia inf ON ii.interferencia_id = inf.id                
                WHERE i.id = %s AND LEFT(i.rotulo, 3) <> '999'
            """, (id_intervencao,))
            cols = ['rotulo', 'vazao_nominal', 'potencia', 'tipo_medidor', 'modo_transmissao', 'codigo_uc']
            return dict(zip(cols, cursor.fetchone())) if cursor.rowcount > 0 else None
        finally:
            cursor.close()

    def limpar_campos_medidor(self):
        """Limpa campos relacionados ao medidor atual."""
        # Mantém o contador atual
        current_medidor = self.medidor_atual
        
        self.limpando_campos = True
        
        self.codigo_medidor_input.clear()
        self.vazao_input.clear()
        self.potencia_input.clear()
        self.equipamento_combo.setCurrentIndex(0)
        self.modo_transmissao_combo.setCurrentIndex(0)
        
        # ✅ Resetar ambas as flags de processamento
        self.vazao_processada = False
        self.potencia_processada = False
        
        self.limpando_campos = False
        self.verificar_campos()
            
        # Restaura o contador após limpeza
        self.medidor_atual = current_medidor
        self.atualizar_interface_medidor()
        
    def limpar_todos_campos(self):
        """Limpa todos os campos da tela de cadastro."""
        self.limpando_campos = True
        
        self.cnarh_input.clear()
        self.outorga_tree.clear()
        self.outorga_label.setVisible(False)
        self.outorga_tree.setVisible(False)
        self.operador_combo.setCurrentIndex(0)
        self.nome_usuario_input.clear()
        self.tree_widget.clear()
        self.limpar_campos_medidor()  # Já reseta ambas as flags
        
        self.limpando_campos = False
        self.verificar_campos()
        
    def processar_atualizacao(self, id_intervencao):
        """Gerencia fluxo de atualização de medidor existente.
        
        Args:
            id_intervencao (int): ID da intervenção a atualizar.
            
        Returns:
            bool: True se atualização foi bem sucedida.
        """
        cursor = None
        try:
            # 1. Obter dados atuais para exibir na confirmação
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT i.rotulo, i.vazao_nominal, i.potencia, otm.nome,
                       tm.descricao, mt.descricao, 
                       inf.codigo_interferencia, cuc.codigo_uc
                FROM tb_intervencao i
                JOIN tb_tipo_medidor tm ON i.tipo_medidor_id = tm.id
                JOIN tb_operador_telemetria otm ON i.operador_telemetria = otm.id
                JOIN tb_modo_transmissao mt ON i.modo_transmissao_id = mt.id
                JOIN tb_intervencao_interferencia ii ON i.id = ii.intervencao_id
                JOIN tb_interferencia inf ON ii.interferencia_id = inf.id
                JOIN tb_codigo_uc_intervencao cuc ON i.id = cuc.intervencao_id
                WHERE i.id = %s;
            """, (id_intervencao,))
            dados = cursor.fetchone()

            if not dados:
                QMessageBox.warning(self, "Aviso", "Dados da intervenção não encontrados.")
                return False

            # 2. Obter a outorga selecionada do tree widget
            outorga_selecionada = self.obter_outorga_selecionada()
            if not outorga_selecionada:
                QMessageBox.warning(self, "Aviso", "Nenhuma outorga selecionada.")
                return False

            # 3. Diálogo de confirmação detalhado
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Question)
            msg.setWindowTitle("Confirmar Atualização")
            msg.setText(f"Deseja atualizar medidor de vazão {dados[0]}?")
            msg.setInformativeText(
                f"Operador atual: {dados[3]} → Novo: {self.operador_combo.currentText()}\n"               
                f"Medidor energia: {dados[7]} → Novo: {self.codigo_medidor_input.text() or 'Não informado'}\n"
                f"Vazão atual: {dados[1]} → Nova: {self.vazao_input.text()}\n"
                f"Potência atual: {dados[2]} → Nova: {self.potencia_input.text()}\n"
                f"Equipamento atual: {dados[4]} → Novo: {self.equipamento_combo.currentText()}\n"
                f"Transmissão atual: {dados[5]} → Nova: {self.modo_transmissao_combo.currentText()}"
            )
            msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            
            if msg.exec_() != QMessageBox.Yes:
                return False

            # 3. Executar a atualização no banco
            sucesso, rotulo = self.atualizar_intervencao(id_intervencao)
            if not sucesso:
                QMessageBox.warning(self, "Erro", f"Falha ao atualizar: {rotulo}")
                return False

            # 4 Atualizar codigo_uc na tb_interferencia
            codigo_uc_novo = self.codigo_medidor_input.text().strip() or None
            codigo_interferencia = dados[6]

            if self.radio_sim.isChecked():
                cursor.execute("""
                    UPDATE tb_interferencia
                    SET codigo_uc = %s
                    WHERE codigo_interferencia = %s
                """, (codigo_uc_novo, codigo_interferencia))
                self.conn.commit()            
            
            else:
                if codigo_uc_novo:
                    cursor.execute("""
                        SELECT codigo_uc FROM tb_interferencia WHERE codigo_interferencia = %s
                    """, (codigo_interferencia,))
                    resultado = cursor.fetchone()

                    if resultado:
                        codigo_uc_atual = resultado[0] or ''
                        lista_ucs = set(filter(None, map(str.strip, codigo_uc_atual.split(','))))
                        lista_ucs.add(codigo_uc_novo)
                        nova_string_uc = ','.join(sorted(lista_ucs))

                        cursor.execute("""
                            UPDATE tb_interferencia
                            SET codigo_uc = %s
                            WHERE codigo_interferencia = %s
                        """, (nova_string_uc, codigo_interferencia))
                        self.conn.commit()

            # 5. Exibir resultado consolidado
            relatorio = {
                'operacao': 'atualizacao',
                'dados_base': {
                    'nome_usuario': self.nome_usuario_input.text(),
                    'operador': self.operador_combo.currentText(),                    
                    'cnarh': self.cnarh_input.text(),
                    'outorga': outorga_selecionada,
                    'codigo_interferencia': dados[6]
                },
                'medidores': [{
                    'numero': 1,
                    'rotulo': rotulo,
                    'vazao': float(self.vazao_input.text()),
                    'potencia': int(self.potencia_input.text()),
                    'equipamento': self.equipamento_combo.currentText(),
                    'transmissao': self.modo_transmissao_combo.currentText(),
                    'codigo_uc': self.codigo_medidor_input.text().strip() or 'Não informado'                    
                }]
            }
            self.exibir_resultado_consolidado(relatorio)
            
            return True

        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Falha no processo de atualização:\n{str(e)}")
            return False
        finally:
            if cursor:
                cursor.close()
            self.limpar_e_resetar()

    def confirmar_operacao(self, operacao, interferencias):
        """Exibe diálogo de confirmação de operação.
        
        Args:
            operacao (str): Tipo de operação ('cadastro' ou 'atualizacao').
            interferencias (list): Lista de interferências envolvidas.
            
        Returns:
            bool: True se usuário confirmou, False caso contrário.
        """
        return QMessageBox.question(
            self,
            f"Confirmar {operacao.title()}",
            f"Confirma o {operacao} com:\n\n"
            f"Interferência(s): {', '.join(interferencias)}\n"
            f"Relógio: {self.codigo_medidor_input.text().strip() or 'Não informado'}\n"
            f"Vazão: {self.vazao_input.text().strip()} m³/s\n"
            f"Potência: {self.potencia_input.text().strip()} kW\n"
            f"Equipamento: {self.equipamento_combo.currentText()}\n"
            f"Transmissão: {self.modo_transmissao_combo.currentText()}",
            QMessageBox.Yes | QMessageBox.No
        ) == QMessageBox.Yes

    def limpar_e_resetar(self):
        """Prepara a interface para um novo cadastro.
        
        Executa:
            - Limpeza de todos os campos
            - Reset de flags de estado
            - Atualização de contadores
            - Exibição de notificação temporária
            - Reativação de validações
        """
        self.limpar_todos_campos()
        self._em_atualizacao = False
        
        if hasattr(self, 'medidores_cadastrados'):
            del self.medidores_cadastrados
            
        if self.radio_nao.isChecked():
            self.medidor_atual = 1
            
        self.notification.setText("Cadastro concluído com sucesso!")
        self.notification.move(350, 550)
        self.notification.show()
        QTimer.singleShot(2000, self.notification.hide)       
        
    def voltar(self):
        """Volta para a tela inicial."""
        self.close()
        self.tela_inicial.show()

