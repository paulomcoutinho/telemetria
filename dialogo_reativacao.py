# -*- coding: utf-8 -*-
"""
Módulo: dialogo_reativacao.py
===============================
Diálogo modal para reativação em lote de medidores desativados.

Um medidor é considerado desativado quando seu rótulo termina com '#'.
O diálogo exibe a lista de medidores desativados para seleção múltipla
e um combo de operadores para nova vinculação.

A reativação é executada via UPDATE atômico usando:
  TRIM(TRAILING '#' FROM rotulo)

Fecha com QDialog.Accepted ao concluir, sinalizando ao chamador
(WidgetMedidores) para recarregar a lista de medidores.

Autor : SFI/ANA
Versão: 2.0 – Março/2026
"""

# ---------------------------------------------------------------------------
# Imports Qt – widgets, layout e utilitários de UI
# ---------------------------------------------------------------------------
from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QComboBox, QListWidget, QListWidgetItem,
    QDialogButtonBox, QFrame, QMessageBox,
    QAbstractItemView,
)
from qgis.PyQt.QtCore import Qt

# ---------------------------------------------------------------------------
# Imports Python padrão
# ---------------------------------------------------------------------------
import psycopg2

# ---------------------------------------------------------------------------
# Sistema de design centralizado
# ---------------------------------------------------------------------------
try:
    from . import ui_tema
except ImportError:
    import ui_tema


class DialogReativacao(QDialog):
    """Diálogo modal para reativação em lote de medidores desativados.

    Um medidor é considerado desativado quando seu rótulo termina com o
    caractere ``#``, convenção utilizada pelo sistema para preservar o
    histórico sem excluir o registro. Este diálogo permite ao operador:
        1. Visualizar a lista de todos os medidores desativados (rótulo
           terminando em ``#``) em um ``QListWidget`` com seleção múltipla;
        2. Selecionar um operador de telemetria ativo no ``QComboBox``
           para vinculação dos medidores reativados;
        3. Confirmar a operação, que executa um ``UPDATE`` em
           ``tb_intervencao`` removendo o ``#`` final via
           ``TRIM(TRAILING '#' FROM rotulo)`` e atribuindo o novo operador
           selecionado.

    A operação é atômica: em caso de erro, o ``rollback`` é executado
    automaticamente e nenhuma alteração parcial é salva. Ao concluir com
    sucesso, o diálogo fecha com ``QDialog.Accepted``, sinalizando ao
    chamador que a lista de medidores deve ser recarregada.

    Attributes:
        conn (psycopg2.connection): Conexão ativa ao PostgreSQL.
        lista_medidores (QListWidget): Lista com seleção múltipla dos
            medidores desativados; cada item armazena o ID do medidor em
            ``Qt.UserRole``.
        combo_operador (QComboBox): Seletor de operador para vinculação,
            carregado a partir de ``tb_operador_telemetria``.
    """
    
    def __init__(self, conexao, parent=None):
        super().__init__(parent)
        self.conn = conexao
        self.setWindowTitle("Reativar Medidores")
        self.setModal(True)
        self.resize(600, 450)
        
        layout = QVBoxLayout(self)
        
        # === SELEÇÃO DE MEDIDORES ===
        label_medidores = QLabel("Medidores Desativados (Selecione um ou mais):")
        label_medidores.setStyleSheet("font-weight: bold; font-size: 12px;")
        layout.addWidget(label_medidores)
        
        self.lista_medidores = QListWidget()
        self.lista_medidores.setSelectionMode(QAbstractItemView.MultiSelection)
        self.lista_medidores.setStyleSheet("""
            QListWidget {
                border: 1px solid #ccc;
                border-radius: 3px;
                background-color: white;
                height: 150px;
            }
            QListWidget::item {
                padding: 4px;
                border-bottom: 1px solid #eee;
            }
            QListWidget::item:selected {
                background-color: #28a745;
                color: white;
            }
        """)
        layout.addWidget(self.lista_medidores)
        
        # === SELEÇÃO DE OPERADOR ===
        label_operador = QLabel("Vincular ao Operador Responsável:")
        label_operador.setStyleSheet("font-weight: bold; font-size: 12px; margin-top: 10px;")
        layout.addWidget(label_operador)
        
        self.combo_operador = QComboBox()
        self.combo_operador.setStyleSheet("""
            QComboBox {
                border: 1px solid #ccc;
                border-radius: 3px;
                padding: 5px;
                background-color: white;
            }
        """)
        layout.addWidget(self.combo_operador)
        
        layout.addStretch()
        
        # === BOTÕES ===
        btn_layout = QHBoxLayout()
        
        btn_confirmar = QPushButton("Reativar Medidores")
        btn_confirmar.setStyleSheet("""
            QPushButton {
                background-color: #28a745;
                color: white;
                font-weight: bold;
                padding: 8px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #218838;
            }
        """)
        btn_confirmar.clicked.connect(self.processar_reativacao)
        
        btn_cancelar = QPushButton("Cancelar")
        btn_cancelar.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                padding: 8px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #5a6268;
            }
        """)
        btn_cancelar.clicked.connect(self.reject)
        
        btn_layout.addWidget(btn_confirmar)
        btn_layout.addWidget(btn_cancelar)
        layout.addLayout(btn_layout)
        
        # Carregar dados iniciais
        self.carregar_dados()

    def carregar_dados(self):
        """Busca medidores com # e operadores no banco."""
        try:
            cursor = self.conn.cursor()
            
            # 1. Buscar medidores desativados (rotulo terminando em #)
            query_medidores = """
            SELECT id, rotulo 
            FROM tb_intervencao 
            WHERE rotulo LIKE '%#' 
            ORDER BY rotulo;
            """
            cursor.execute(query_medidores)
            medidores = cursor.fetchall()
            
            self.lista_medidores.clear()
            for id_medidor, rotulo in medidores:
                item = QListWidgetItem(rotulo)
                item.setData(Qt.UserRole, id_medidor)
                self.lista_medidores.addItem(item)
            
            # 2. Buscar Operadores
            query_operadores = """
            SELECT id, nome, email 
            FROM tb_operador_telemetria 
            ORDER BY nome ASC;
            """
            cursor.execute(query_operadores)
            operadores = cursor.fetchall()
            
            self.combo_operador.clear()
            self.combo_operador.addItem(" -- Selecione um Operador -- ", None)
            for id_op, nome, email in operadores:
                display_text = f"{nome}"
                if email:
                    display_text += f" ({email})"
                self.combo_operador.addItem(display_text, id_op)
                
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao carregar dados de reativação: {e}")
        finally:
            cursor.close()

    def processar_reativacao(self):
        """Executa o UPDATE de reativação."""
        itens_selecionados = self.lista_medidores.selectedItems()
        id_operador = self.combo_operador.currentData()
        
        if not itens_selecionados:
            QMessageBox.warning(self, "Atenção", "Selecione pelo menos um medidor para reativar.")
            return
            
        if id_operador is None:
            QMessageBox.warning(self, "Atenção", "Selecione um operador responsável.")
            return
            
        ids_selecionados = [item.data(Qt.UserRole) for item in itens_selecionados]
        qtd = len(ids_selecionados)
        
        # Confirmação
        resposta = QMessageBox.question(
            self, "Confirmar Reativação", 
            f"Deseja reativar {qtd} medidor(es) e vinculá-los ao operador selecionado?\n\n"
            "Isso removerá o '#' do rótulo e atribuirá o operador.",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        
        if resposta != QMessageBox.Yes:
            return
            
        try:
            cursor = self.conn.cursor()
            
            # Query de Reativação
            # Remove o '#' do final do rótulo usando TRIM e atualiza o operador
            query = """
            UPDATE tb_intervencao 
            SET 
                rotulo = TRIM(TRAILING '#' FROM rotulo),
                operador_telemetria = %s
            WHERE id = ANY(%s);
            """
            
            # Passamos a lista de IDs e o ID do operador
            cursor.execute(query, (id_operador, ids_selecionados))
            
            self.conn.commit()
            
            QMessageBox.information(self, "Sucesso", f"{qtd} medidor(es) reativado(s) com sucesso!")
            self.accept() # Fecha o dialogo retornando Accepted
            
        except Exception as e:
            self.conn.rollback()
            QMessageBox.critical(self, "Erro", f"Erro ao reativar medidores: {e}")
        finally:
            cursor.close()

