# -*- coding: utf-8 -*-
"""
Módulo: tela_cadastro_operadores.py
====================================
Formulário de cadastro de operadores de telemetria no banco
de dados DURH Diária (tabela tb_operador_telemetria).

Suporta dois modos mutuamente exclusivos:
  - Operador é o próprio usuário de água (busca automática no CNARH via API REST);
  - Operador é terceirizado (preenchimento manual completo).

Autor : SFI/ANA
Versão: 2.0 – Março/2026
"""

# ---------------------------------------------------------------------------
# Imports Qt – widgets, layout e utilitários de UI
# ---------------------------------------------------------------------------
from qgis.PyQt.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout,
    QLabel, QLineEdit, QPushButton, QRadioButton, QButtonGroup,
    QTextEdit, QDateEdit, QFrame, QMessageBox, QSizePolicy,
    QDesktopWidget, QApplication,
)
from qgis.PyQt.QtCore import Qt, QDate, QRegExp, QTimer, QMimeData
from qgis.PyQt.QtGui import QRegExpValidator

# ---------------------------------------------------------------------------
# Imports Python padrão
# ---------------------------------------------------------------------------
import re
import psycopg2
import urllib.request
import urllib.parse
import ssl

# ---------------------------------------------------------------------------
# Sistema de design centralizado
# ---------------------------------------------------------------------------
try:
    from . import ui_tema
except ImportError:
    import ui_tema


class TelaCadastroOperadores(QWidget):
    """Tela para cadastro de operadores de telemetria no sistema DURH Diária por Telemetria.

    Permite o cadastro de novos operadores responsáveis pela telemetria,
    com validação de dados e integração com o banco de dados PostgreSQL.
    Oferece duas modalidades de cadastro:
    - Operador é o próprio usuário de água OU
    - Operador é terceirizado (dados completos necessários)

    Attributes:
        tela_inicial (QWidget): Referência à tela principal do plugin.
        conn (psycopg2.connection): Conexão ativa com o banco de dados.
        nome_usuario (str): Nome do usuário obtido da consulta ao CNARH.
        
    Widgets Principais:
        radio_sim (QRadioButton): Opção "Operador é o usuário de água".
        radio_nao (QRadioButton): Opção "Operador é terceiro".
        nome_input (QLineEdit): Campo para nome do operador.
        cpf_cnpj_input (QLineEdit): Campo para CPF/CNPJ ou código CNARH.
        email_input (QLineEdit): Campo para e-mail com validação.
        data_input (QDateEdit): Seletor de data de cadastro.
        resultado_label (QTextEdit): Área de exibição do resultado do cadastro.
        cadastrar_btn (QPushButton): Botão para acionar o cadastro.
        voltar_btn (QPushButton): Botão para retornar à tela anterior.

    Flags de Estado:
        _em_consulta (bool): Indica quando uma consulta ao CNARH está em andamento.
        _dados_validados (bool): Indica se os dados atuais passaram na validação.
    """
    def __init__(self, tela_inicial, conexao):
        """Inicializa a tela de cadastro de operadores com design moderno."""   
        super().__init__()
        self.tela_inicial = tela_inicial       
        self.setWindowTitle("Cadastro de Operador - DURH Diária por Telemetria")
        self.setFixedSize(450, 450)
        self.conn = conexao        
        self.center()
        
        ui_tema.aplicar_tema_arredondado(self)
               
        # Layout principal
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(15, 15, 15, 15)
        #main_layout.setSpacing(15)
        
        # Título
        #titulo = QLabel("Novo Cadastro de Operador")
        #titulo.setStyleSheet(f"font-size: 18px; font-weight: bold; color: {ui_tema.StyleConfig.PRIMARY_COLOR};")
        #main_layout.addWidget(titulo)
        
        # Container Branco para o formulário
        form_container = QFrame()
        form_container.setObjectName("ContainerBranco")
        form_layout = QVBoxLayout(form_container)
        form_layout.setContentsMargins(20, 20, 20, 20)
        #form_layout.setSpacing(15)
        
        # Pergunta com RadioButtons
        self.pergunta_label = QLabel("O operador é o próprio usuário de água?")
        self.pergunta_label.setStyleSheet("font-weight: bold;")
        form_layout.addWidget(self.pergunta_label)
        
        radio_layout = QHBoxLayout()
        self.radio_sim = QRadioButton("Sim")
        self.radio_nao = QRadioButton("Não")
        self.radio_nao.setChecked(True)
        self.radio_sim.toggled.connect(self.atualizar_interface)
        radio_layout.addWidget(self.radio_sim)
        radio_layout.addWidget(self.radio_nao)
        radio_layout.addStretch()
        form_layout.addLayout(radio_layout)
        
        # Campo para o nome
        self.nome_label = QLabel("Nome Completo:")
        self.nome_input = QLineEdit()
        self.nome_input.setPlaceholderText("Digite o nome do operador")
        self.nome_input.setFixedHeight(30)        
        form_layout.addWidget(self.nome_label)
        form_layout.addWidget(self.nome_input)
        
        # Campo para CPF/CNPJ
        self.cpf_cnpj_label = QLabel("CPF ou CNPJ:")
        self.cpf_cnpj_input = QLineEdit()
        self.cpf_cnpj_input.setPlaceholderText("Apenas números")
        self.cpf_cnpj_input.setFixedHeight(30)
        self.cpf_cnpj_input.setMaxLength(15)
        regex_numeros = QRegExp("^[0-9]{0,15}$")
        self.cpf_cnpj_input.setValidator(QRegExpValidator(regex_numeros, self))
        form_layout.addWidget(self.cpf_cnpj_label)
        form_layout.addWidget(self.cpf_cnpj_input)
        
        # Campo para e-mail
        self.email_label = QLabel("E-mail de Contato:")
        self.email_input = QLineEdit()
        self.email_input.setPlaceholderText("exemplo@email.com")
        self.email_input.setFixedHeight(30)
        regex_email = QRegExp("^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$")
        self.email_input.setValidator(QRegExpValidator(regex_email, self))
        form_layout.addWidget(self.email_label)
        form_layout.addWidget(self.email_input)
        
        # Campo para data
        self.data_label = QLabel("Data do Cadastro:")
        self.data_input = QDateEdit(QDate.currentDate())
        self.data_input.setFixedHeight(30)
        self.data_input.setCalendarPopup(True)
        form_layout.addWidget(self.data_label)
        form_layout.addWidget(self.data_input)
        
        main_layout.addWidget(form_container)
        
        # Botões de ação
        btn_layout = QHBoxLayout()
        self.cadastrar_btn = QPushButton("Cadastrar")
        self.cadastrar_btn.clicked.connect(self.cadastrar)
        
        self.voltar_btn = QPushButton("Voltar")
        self.voltar_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: transparent;
                color: {ui_tema.StyleConfig.SECONDARY_COLOR};
                border: 1px solid {ui_tema.StyleConfig.SECONDARY_COLOR};
            }}
            QPushButton:hover {{ background-color: #f0f0f0; }}
        """)
        self.voltar_btn.clicked.connect(self.voltar)
        
        btn_layout.addStretch()
        btn_layout.addWidget(self.voltar_btn)
        btn_layout.addWidget(self.cadastrar_btn)
        main_layout.addLayout(btn_layout)
        
        # Área de resultado (discreta)
        self.resultado_label = QTextEdit()
        self.resultado_label.setReadOnly(True)
        self.resultado_label.setMaximumHeight(50)
        self.resultado_label.setStyleSheet("border: none; background: transparent; color: #666;")
        main_layout.addWidget(self.resultado_label)
        
        # Botão de copiar para email
        self.copiar_email_btn = QPushButton("Copiar para Email")
        self.copiar_email_btn.clicked.connect(self.copiar_texto_operador_email)
        self.copiar_email_btn.setVisible(False)
        main_layout.addWidget(self.copiar_email_btn)
        
        self.setLayout(main_layout)

        self.limpando_campos = False
        self.cadastrar_btn.setDisabled(True)
        self.nome_input.textChanged.connect(self.verificar_campos)
        self.cpf_cnpj_input.editingFinished.connect(self.buscar_nome_operador_auto) 
        self.cpf_cnpj_input.textChanged.connect(self.verificar_campos)        
        self.email_input.textChanged.connect(self.verificar_campos)
        self.radio_sim.toggled.connect(self.verificar_campos)
        self.radio_nao.toggled.connect(self.verificar_campos)

    def validar_email(self, email):
        """Valida o formato de um endereço de e-mail usando regex.
        
        Args:
            email (str): Endereço de e-mail a ser validado.
            
        Returns:
            bool: True se o e-mail é válido, False caso contrário.
            
        Padrão aceito:
            usuario@dominio.extensao
            Permite letras, números, pontos, hífens e underscores
        """        
        if not email:
            return False

        # Regex para validação básica de email
        regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        return re.match(regex, email) is not None

    def center(self):
        """Centraliza a janela na tela."""        
        screen_geometry = QDesktopWidget().screenGeometry()  # Obtém a geometria da tela
        center_point = screen_geometry.center()  # Obtém o ponto central da tela
        frame_geometry = self.frameGeometry()  # Obtém a geometria da janela
        frame_geometry.moveCenter(center_point)  # Move o centro da janela para o centro da tela
        self.move(frame_geometry.topLeft())  # Move a janela para a posição correta

    def atualizar_interface(self):
        """Ajusta a interface conforme o tipo de operador selecionado.
        
        Quando 'Operador é o usuário de água' (radio_sim):
        - Desabilita o campo de nome
        - Altera o label para "Código CNARH"
        
        Quando 'Operador é terceiro' (radio_nao):
        - Habilita o campo de nome
        - Altera o label para "CPF/CNPJ"
        
        Trigger:
            - Mudança no estado dos radio buttons
        """
        if self.radio_sim.isChecked():
            self.nome_input.setDisabled(True)
            self.cpf_cnpj_label.setText("Código CNARH (Ex: 310005248163): ")
        else:
            self.nome_input.setDisabled(False)
            self.cpf_cnpj_label.setText("CPF/CNPJ: ")
            # Limpa o nome quando mudar para radio_nao
            self.nome_input.clear()
        
        # ✅ NOVO: Revalidar campos após mudança de interface
        self.verificar_campos()           

    def buscar_nome_operador(self):
        """Consulta o nome do usuário associado ao CNARH no banco de dados.
        
        Returns:
            str: Nome do operador encontrado ou None se não existir.
            
        Fluxo:
            1. Valida se o radio_sim está marcado
            2. Obtém o CNARH do campo cpf_cnpj_input
            3. Consulta tb_mapserver_obrigatoriedade
            4. Atualiza nome_input se encontrado
            
        Exibe:
            - Mensagem de erro se CNARH não encontrado (apenas se campo estiver completo)
        """
        if self.radio_sim.isChecked():
            cnarh = self.cpf_cnpj_input.text().strip()
            
            # ✅ NÃO busca se campo estiver vazio (usuário ainda está digitando)
            if not cnarh or len(cnarh) < 10:
                return None
            
            if cnarh:
                try:
                    cursor = self.conn.cursor()
                    query = """
                    SELECT nome_usuario FROM tb_mapserver_obrigatoriedade
                    WHERE numero_cadastro = %s;
                    """
                    cursor.execute(query, (cnarh,))
                    resultado = cursor.fetchone()
                    
                    if resultado:
                        nome_operador = resultado[0]
                        self.nome_input.setText(nome_operador)
                        
                        self.verificar_campos()
                        
                        # ✅ Retorna o nome para confirmar sucesso
                        return nome_operador
                    else:
                        # ✅ NÃO exibe mensagem de erro durante a digitação
                        # Apenas limpa o campo de nome
                        self.nome_input.clear()

                        self.verificar_campos()

                        return None
                except Exception as e:
                    print(f"Erro ao buscar nome do operador: {e}")
                    return None
                finally:
                    cursor.close()
        
        return None
        
    def buscar_nome_operador_auto(self):
        """
        Busca automaticamente o nome do operador quando o campo CNARH/CPF perde o foco.
        
        Só executa a busca quando:
        - Radio 'Sim' está marcado (operador é o próprio usuário)
        - Campo CNARH não está vazio
        - Campo tem pelo menos 10 dígitos (CNARH válido)
        
        Após buscar, atualiza o estado do botão 'Cadastrar'.
        """
        # Só busca se radio_sim estiver marcado
        if not self.radio_sim.isChecked():
            return
        
        cnarh = self.cpf_cnpj_input.text().strip()
        
        # Validação mínima: CNARH deve ter pelo menos 10 dígitos
        if len(cnarh) < 10:
            return
        
        # Chama o método existente de busca
        nome_encontrado = self.buscar_nome_operador()

        if not nome_encontrado:
            QMessageBox.warning(
                self,
                "CNARH Não Encontrado",
                f"O código CNARH informado ({cnarh}) não foi encontrado na base de dados.\n\n"
                "Verifique se o número está correto e tente novamente.\n\n"
                "Caso o problema persista, entre em contato com o administrador do sistema."
            )
            self.cpf_cnpj_input.setFocus()
            self.cpf_cnpj_input.selectAll()
        
        # ✅ IMPORTANTE: Atualiza o estado do botão após buscar
        self.verificar_campos()

    def verificar_existencia(self, email, nome_operador=None):
        """Verifica se operador já está cadastrado no sistema.
        
        Args:
            nome_operador (str): Nome completo do operador.
            email (str): Endereço de e-mail do operador.
            
        Returns:
            bool: True se operador já existe, False caso contrário.
            
        Lógica:
            - Se radio_sim: verifica apenas por e-mail
            - Se radio_nao: verifica por nome OU e-mail
        """
        try:
            cursor = self.conn.cursor()
            # Define a query com base na escolha do RadioButton
            if self.radio_sim.isChecked():
                query = """
                SELECT COUNT(*) FROM tb_operador_telemetria
                WHERE email = %s;
                """
                cursor.execute(query, (email,))
            else:
                query = """
                SELECT COUNT(*) FROM tb_operador_telemetria
                WHERE nome = %s OR email = %s;
                """
                cursor.execute(query, (nome_operador, email))
            # Obtém o resultado da consulta
            resultado = cursor.fetchone()[0]
            return resultado > 0  # Retorna True se já existir algum registro
        except Exception as e:
            print(f"Erro ao verificar existência: {e}")
            return False
        finally:
            cursor.close()

    def verificar_campos(self):
        """
        Verifica se todos os campos obrigatórios estão preenchidos.
        
        Campos obrigatórios:
        - Nome Completo (sempre, mas pode estar desabilitado se radio_sim)
        - E-mail (sempre)
        - CPF/CNPJ ou CNARH (depende do radio button selecionado)
        
        Exceções (NÃO validados):
        - Data do Cadastro (já possui valor padrão)
        """
        if self.limpando_campos:
            return  # Não valida durante a limpeza dos campos
        
        # Verifica se há um operador selecionado (radio buttons)
        operador_selecionado = self.radio_sim.isChecked() or self.radio_nao.isChecked()
        
        # Campo Nome: sempre obrigatório
        nome_preenchido = bool(self.nome_input.text().strip())
        
        # Campo E-mail: sempre obrigatório E deve ter formato válido
        email_texto = self.email_input.text().strip()
        email_preenchido = bool(email_texto)
        email_valido = self.validar_email(email_texto) if email_preenchido else False
        
        # Campo CPF/CNPJ ou CNARH: depende do radio button
        documento_preenchido = bool(self.cpf_cnpj_input.text().strip())
        
        # ✅ Habilita botão apenas se TODOS os campos estiverem preenchidos
        # E O EMAIL TIVER FORMATO VÁLIDO
        self.cadastrar_btn.setEnabled(
            operador_selecionado and
            nome_preenchido and
            email_preenchido and
            email_valido and      # ✅ NOVO: Validação de formato do email
            documento_preenchido
        )
        
    def obter_proximo_rotulo(self):
        """Gera o próximo rótulo disponível para medidores fictícios.
        
        Returns:
            str: Rótulo no formato "999XXX" onde XXX é um número sequencial.
            
        Consulta:
            - Máximo valor existente na tb_intervencao com prefixo 999
            - Incrementa 1 ao valor encontrado
        """
        try:
            cursor = self.conn.cursor()
            query = """
            SELECT COALESCE(MAX(CAST(SUBSTRING(rotulo FROM '[0-9]+$') AS INTEGER)), 0)
            FROM tb_intervencao 
            WHERE rotulo LIKE '999%';
            """
            cursor.execute(query)
            resultado = cursor.fetchone()[0]
            proximo_valor = resultado + 1
            return f"999{proximo_valor % 1000:03d}"  # Formata como 999xxx
        except Exception as e:
            print(f"Erro ao obter próximo rótulo: {e}")
            return None
        finally:
            cursor.close()

    def cadastrar_medidor_ficticio(self, operador_id):
        """Cria um medidor fictício associado ao operador.
        
        Args:
            operador_id (int): ID do operador no banco de dados.
            
        Returns:
            tuple: (id_intervencao, rótulo) ou None em caso de erro.
            
        Dados fixos:
            - modo_transmissao_id: 6 (indefinido)
            - vazao_nominal: 0
            - potencia: 0
            - tipo_medidor_id: 1 (genérico)
            - Coordenadas: NULL
        """
        try:
            # Obtém o próximo rótulo disponível
            rotulo = self.obter_proximo_rotulo()
            if not rotulo:
                raise ValueError("Não foi possível gerar o próximo rótulo.")

            # Define os valores fixos para o medidor fictício
            modo_transmissao_id = 6
            vazao_nominal = 0
            potencia = 0
            tipo_medidor_id = 1
            longitude = None
            latitude = None
            material_tubulacao_id = 5
            espessura_tubulacao = 0
            diametro_tubulacao = 0
            shape = None

            # Insere o medidor fictício na tabela tb_intervencao
            cursor = self.conn.cursor()
            query = """
            INSERT INTO tb_intervencao (
                modo_transmissao_id, vazao_nominal, potencia, rotulo, tipo_medidor_id,
                operador_telemetria, longitude, latitude, material_tubulacao_id,
                espessura_tubulacao, diametro_tubulacao, shape
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING id;
            """
            cursor.execute(query, (
                modo_transmissao_id, vazao_nominal, potencia, rotulo, tipo_medidor_id,
                operador_id, longitude, latitude, material_tubulacao_id,
                espessura_tubulacao, diametro_tubulacao, shape
            ))
            intervencao_id = cursor.fetchone()[0] 
            self.conn.commit()
            return intervencao_id, rotulo
        except Exception as e:
            self.conn.rollback()
            print(f"Erro ao cadastrar medidor fictício: {e}")
            return None
        finally:
            cursor.close()

    def cadastrar_intervencao_interferencia(self, intervencao_id, interferencia_id):
        """Cadastra relação entre intervenção e interferência.
        
        Args:
            intervencao_id (int): ID da intervenção.
            interferencia_id (int): ID da interferência.
        """
        try:
            cursor = self.conn.cursor()
            query = """
            INSERT INTO tb_intervencao_interferencia (intervencao_id, interferencia_id)
            VALUES (%s, %s);
            """
            cursor.execute(query, (intervencao_id, interferencia_id))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Erro ao cadastrar intervenção-interferência: {e}")
        finally:
            cursor.close()

    def cadastrar(self):
        """Fluxo completo de cadastro de um novo operador.
        
        Fluxo principal:
        1. Validação dos campos obrigatórios
        2. Consulta CNARH (se radio_sim)
        3. Validação de e-mail
        4. Verificação de duplicidade (por número de cadastro)
        5. Se existir, oferece opção de atualizar e-mail
        6. Confirmação com usuário
        7. Inserção no banco em transação:
           - tb_operador_telemetria
           - tb_interferencia (registro teste)
           - tb_intervencao (medidor fictício)
           - tb_intervencao_interferencia
        8. Exibição do resultado
        
        Tratamento de erros:
            - Rollback em caso de falha
            - Mensagens específicas por tipo de erro
        """
        operador_proprio_usuario = self.radio_sim.isChecked()

        # Verifica se o campo CNARH está vazio quando o RadioButton está marcado como "Sim"
        if operador_proprio_usuario:
            cnarh = self.cpf_cnpj_input.text().strip()
            if not cnarh:
                QMessageBox.warning(self, "Campo Vazio", "O campo 'Código CNARH' é obrigatório.")
                return

        # Obtém o nome do operador
        if operador_proprio_usuario:
            nome_operador = self.buscar_nome_operador()
            if not nome_operador:
                return
        else:
            nome_operador = self.nome_input.text().strip()
            if not nome_operador:
                QMessageBox.warning(self, "Campo Vazio", "O campo 'Nome do Operador' é obrigatório.")
                return

        # Obtém os valores dos campos
        cpf_cnpj = self.cpf_cnpj_input.text().strip()
        email = self.email_input.text().strip()            
        data = self.data_input.date().toString("dd/MM/yyyy")

        # Define o número de cadastro
        numero_cadastro = cpf_cnpj if not operador_proprio_usuario else None

        # Verifica campos obrigatórios
        if not cpf_cnpj and not operador_proprio_usuario:
            QMessageBox.warning(self, "Campo Vazio", "O campo 'CPF/CNPJ' é obrigatório.")
            return
        if not email:
            QMessageBox.warning(self, "Campo Vazio", "O campo 'E-mail' é obrigatório.")
            return

        # Validação do e-mail
        if not self.validar_email(email):
            QMessageBox.warning(self, "E-mail Inválido", "O e-mail digitado não é válido. Por favor, insira um e-mail no formato correto.")
            return

        # Verifica se o operador já existe
        if self.verificar_existencia(email):
            QMessageBox.warning(self, "Operador Existente", "O operador já está cadastrado na base de dados.")
            return

        # Verifica se já existe um operador com o mesmo número CPF/CNPJ
        try:
            n_cpf_cnpf = cpf_cnpj if not operador_proprio_usuario else cpf_cnpj
            
            cursor = self.conn.cursor()
            query = """
            SELECT id, nome, email FROM tb_operador_telemetria
            WHERE numero_cadastro = %s;
            """
            cursor.execute(query, (n_cpf_cnpf,))
            operador_existente = cursor.fetchone()

            if operador_existente:
                id_existente, nome_existente, email_existente = operador_existente
                resposta = QMessageBox.question(
                    self,
                    "Operador Existente",
                    f"Já existe um operador cadastrado com este número de CPF/CNPJ:\n\n"
                    f"Nome: {nome_existente}\n"
                    f"E-mail atual: {email_existente}\n\n"
                    "Deseja atualizar o e-mail do cadastro existente?",
                    QMessageBox.Yes | QMessageBox.No, QMessageBox.No
                )
                
                if resposta == QMessageBox.Yes:
                    # Atualiza apenas o e-mail
                    try:
                        update_query = """
                        UPDATE tb_operador_telemetria
                        SET email = %s
                        WHERE id = %s;
                        """
                        cursor.execute(update_query, (email, id_existente))

                        rotulo_query = """
                        SELECT b.rotulo
                        FROM tb_intervencao b
                        WHERE b.operador_telemetria = %s
                        LIMIT 1;
                        """
                        cursor.execute(rotulo_query, (id_existente,))
                        rotulo_resultado = cursor.fetchone()
                        rotulo = rotulo_resultado[0] if rotulo_resultado else "N/A"

                        self.conn.commit()

                        # Atualiza o componente de resultado final
                        resultado_texto = f"""
                        Operador/usuário: <b>{nome_existente}</b> ({email})<br><br>
                        ID_teste: <b style="background-color: #f0f0f0;">{rotulo}</b>
                        """
                        self.resultado_label.setHtml(resultado_texto)
                        
                        QMessageBox.information(
                            self,
                            "E-mail Atualizado",
                            f"O e-mail do operador {nome_existente} foi atualizado com sucesso para:\n{email}"
                        )
                        return
                    except Exception as e:
                        self.conn.rollback()
                        QMessageBox.critical(self, "Erro", f"Erro ao atualizar e-mail: {e}")
                        return
                else:
                    return
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao verificar operador existente: {e}")
            return
        finally:
            if cursor:
                cursor.close()
                self.limpar_e_resetar()

        # Restante do fluxo de cadastro
        mensagem = f"""
        ATENÇÃO! Confirma o cadastro das informações abaixo?
        
        Nome do operador/usuário: {nome_operador}
        CPF/CNPJ: {numero_cadastro if numero_cadastro else "Não aplicável"}
        E-mail: {email}
        Data de Cadastro: {data}
        """
        confirmacao = QMessageBox.question(
            self, "Confirmação de Cadastro", mensagem,
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        if confirmacao != QMessageBox.Yes:
            return

        # Insere os dados no banco
        cursor = None
        try:
            cursor = self.conn.cursor()

            # Insere na tabela tb_operador_telemetria
            query_operador = """
            INSERT INTO tb_operador_telemetria (nome, numero_cadastro, email, data)
            VALUES (%s, %s, %s, %s) RETURNING id;
            """
            cursor.execute(query_operador, (nome_operador, numero_cadastro, email, data))
            operador_id = cursor.fetchone()[0]

            # Insere na tabela tb_interferencia
            query_interferencia = """
            INSERT INTO tb_interferencia (codigo_interferencia, nome_usuario, numero_cadastro)
            VALUES (%s, %s, %s) RETURNING id;
            """
            cursor.execute(query_interferencia, ("TESTE", nome_operador, "TESTE"))
            interferencia_id = cursor.fetchone()[0]

            # Insere na tabela tb_intervencao
            intervencao_id, rotulo = self.cadastrar_medidor_ficticio(operador_id)
            if not intervencao_id:
                raise ValueError("Falha ao cadastrar medidor fictício.")

            # Insere na tabela tb_intervencao_interferencia
            if interferencia_id:
                self.cadastrar_intervencao_interferencia(intervencao_id, interferencia_id)

            self.conn.commit()
            
            # Atualiza o componente de resultado final
            resultado_texto = f"""
            Operador/usuário: <b>{nome_operador}</b> ({email})<br><br>
            ID_teste: <b style="background-color: #f0f0f0;">{rotulo}</b>
            """
            self.resultado_label.setHtml(resultado_texto)
            
            QMessageBox.information(self, "Sucesso", "Dados cadastrados com sucesso!")
            self.copiar_email_btn.setVisible(True)
            
        except Exception as e:
            self.conn.rollback()
            QMessageBox.critical(self, "Erro", f"Erro ao cadastrar dados: {e}")
        finally:
            if cursor:
                cursor.close()
            
    def to_bold_unicode(self, texto: str) -> str:
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

    def copiar_texto_operador_email(self):
        """Copia o texto do resultado do cadastro de operadores para a área de transferência no formato de email."""
        try:
            # Extrair informações do resultado_label
            texto_html = self.resultado_label.toHtml()
            
            # Extrair nome do operador, email e ID_teste do HTML
            nome_operador = ""
            email_operador = ""
            id_teste = ""
            
            # Debug: Ver o conteúdo real do HTML
            print("HTML do resultado_label:", texto_html)
            
            # Padrão correto baseado no resultado_texto do método cadastrar()
            # Padrão 1: "Operador/usuário: <b>{nome_operador}</b> ({email})<br><br>"
            # Padrão 2: "ID_teste: <b style="background-color: #f0f0f0;">{rotulo}</b>"
            
            # Extrair nome do operador e email - padrão correto
            nome_match = re.search(r'Operador/usuário:.*?<b>(.*?)</b>.*?\((.*?)\)', texto_html, re.DOTALL)
            if nome_match:
                nome_operador = nome_match.group(1).strip()
                email_operador = nome_match.group(2).strip()
                print(f"Encontrado - Nome: {nome_operador}, Email: {email_operador}")
            else:
                # Tentar padrão alternativo
                nome_match_alt = re.search(r'Operador/usuário:.*?<b[^>]*>(.*?)</b>', texto_html, re.DOTALL)
                if nome_match_alt:
                    nome_operador = nome_match_alt.group(1).strip()
                    print(f"Nome encontrado (alternativo): {nome_operador}")
            
            # Extrair ID_teste - padrão correto
            id_match = re.search(r'ID_teste:.*?<b[^>]*>(.*?)</b>', texto_html, re.DOTALL)
            if id_match:
                id_teste = id_match.group(1).strip()
                print(f"ID_teste encontrado: {id_teste}")
            
            # Se não encontrou via regex, tentar método alternativo
            if not nome_operador or not email_operador or not id_teste:
                texto_simples = self.resultado_label.toPlainText()
                print("Texto simples:", texto_simples)
                
                # Tentar extrair do texto simples
                linhas = texto_simples.split('\n')
                for linha in linhas:
                    if 'Operador/usuário:' in linha:
                        partes = linha.split('(')
                        if len(partes) > 1:
                            nome_operador = partes[0].replace('Operador/usuário:', '').strip()
                            email_operador = partes[1].replace(')', '').strip()
                    elif 'ID_teste:' in linha:
                        id_teste = linha.replace('ID_teste:', '').strip()
            
            # Garantir que temos os valores
            if not nome_operador:
                nome_operador = "Nome não encontrado"
            if not email_operador:
                # Usar o email do campo de entrada como fallback
                email_operador = self.email_input.text().strip()
            if not id_teste:
                id_teste = "ID não encontrado"
            
            print(f"Valores finais - Nome: {nome_operador}, Email: {email_operador}, ID: {id_teste}")
            
            # Gerar senha temporária (exemplo)
            senha_temporaria = "123456"  # Senha inicial padrão
            
            # Construir texto no formato de email
            texto_com_asteriscos = "Senhor(a), \n"
            texto_com_asteriscos += "Informamos que o seu cadastro de operador de telemetria foi realizado.  \n\n"
            
            texto_com_asteriscos += f"Usuário/Operador: **{email_operador}**\n"
            texto_com_asteriscos += f"Senha: **{senha_temporaria}**\n"
            texto_com_asteriscos += f"ID_teste: **{id_teste}**\n\n"
            
            texto_com_asteriscos += "Acesse o link abaixo e realize a troca de senha:\n"
            texto_com_asteriscos += "**https://sso.snirh.gov.br/realms/ana/account/#/security/signingin**\n\n"
            
            texto_com_asteriscos += "Para testar o envio de dados utilize o ID_teste informado acima e as orientações do documento anexo. \n\n"
            
            texto_com_asteriscos += "Observe orientações e padrões de dados em **https://automonitoramento.ana.gov.br** e em **https://telemetria.snirh.gov.br** \n\n"
            
            texto_com_asteriscos += "Após realizar a alteração de senha e testes de envio, preencha o cadastro de medidores reais para as outorgas com obrigatoriedade à telemetria no formulário disponível no portal **https://automonitoramento.ana.gov.br**. \n\n"
            
            texto_com_asteriscos += "Atenciosamente,"
            
            # Função para converter texto entre asteriscos em negrito Unicode
            def bold_replacer(match):
                return self.to_bold_unicode(match.group(1))
            
            # Converter todos os textos entre ** para negrito Unicode
            texto_formatado = re.sub(r"\*\*(.*?)\*\*", bold_replacer, texto_com_asteriscos)
            
            # Copiar para área de transferência usando QMimeData
            mime_data = QMimeData()
            mime_data.setText(texto_formatado)
            clipboard = QApplication.clipboard()
            clipboard.setMimeData(mime_data)

            # Mostrar mensagem de confirmação
            result = QMessageBox.information(
                self,
                "Texto para e-mail",
                "O texto do operador foi copiado para a área de transferência!",
                QMessageBox.Ok
            )
            
            # Após clicar em OK, limpar tudo e voltar ao início
            if result == QMessageBox.Ok:
                self.limpar_e_resetar()
                
        except Exception as e:
            print(f"Erro detalhado: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.warning(
                self,
                "Erro ao Copiar",
                f"Não foi possível copiar o texto:\n{str(e)}"
            )
            
    def limpar_todos_campos(self):
        """Limpa todos os campos da tela de cadastro."""
        self.limpando_campos = True
        
        self.nome_input.clear()
        self.cpf_cnpj_input.clear()
        self.email_input.clear()
        self.radio_nao.setChecked(True)
        self.resultado_label.clear()
        self.copiar_email_btn.setVisible(False)        

        self.limpando_campos = False
        self.verificar_campos()

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
            
    def voltar(self):
        """Fecha a tela atual e retorna à tela principal.
        
        Mantém a conexão com o banco de dados aberta para reutilização.
        """ 
        self.close()
        self.tela_inicial.show()

