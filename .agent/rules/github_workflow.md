# Git Workflow — Commit + Push Automático

## Regra principal

Após **qualquer modificação em arquivo do projeto** — `.py`, `.md`, `.txt`,
criação de subpastas, renomeação ou exclusão — o agente **sugere** o commit
e aguarda confirmação explícita do usuário antes de executar.

O agente apresenta a sugestão no formato:

```
📦 Pronto para commit. Confirma?

  git add -A
  git commit -m "fix(widget_medidores): corrige sanitização de filename"
  git push origin main

  Arquivos afetados:
    M  widget_medidores.py
    M  .agent/rules/github_workflow.md

  [S para confirmar / N para cancelar / E para editar a mensagem]
```

O push só é executado após resposta afirmativa (`s`, `sim`, `y`, `yes`,
ou qualquer confirmação equivalente). Resposta negativa descarta sem alterar
o working tree.

Isso se aplica a:
- Edição de arquivos `.py` existentes (correções, refatorações, novas funções)
- Criação de novos arquivos (qualquer extensão)
- Criação de subpastas com conteúdo
- Atualização de `.md`, `.txt`, `requirements.txt`, `.env.example`
- Exclusão de arquivos (com `git rm`)

## Sequência obrigatória após cada modificação

```bash
cd <raiz-do-projeto>          # sempre a raiz do clone local
git add -A                    # inclui novos arquivos, exclusões e renomeações
git commit -m "<mensagem>"
git push origin <branch-atual>
```

Nunca usar `git add <arquivo>` individual — usar sempre `git add -A` para
capturar criações de subpastas e movimentações junto com a edição principal.

## Formato da mensagem de commit

```
<tipo>(<escopo>): <descrição curta em português>
```

**Tipos**:
- `fix` — correção de bug ou comportamento incorreto
- `feat` — nova funcionalidade ou novo arquivo
- `refactor` — reorganização sem mudança de comportamento
- `docs` — alteração exclusiva em `.md` ou `.txt`
- `chore` — arquivos de configuração, dependências, estrutura de pastas

**Escopo**: nome do arquivo principal modificado sem extensão, ou nome da pasta.

**Exemplos**:
```
fix(widget_atualizacao_base): corrige fallback DELETE FROM em TRUNCATE
feat(calc_mes_thread): adiciona detecção de wrap-around acumulado
refactor(widget_medidores): extrai helper de sanitização de filename
docs(04_modelo_dados_postgis): atualiza glossário com termo UGRH
chore(.agent/skills): cria skill 07_chatbot_mcp
feat(mcp_server): adiciona ferramenta excedencias_outorgado
```

## Verificações antes do push

Antes de executar `git push`, o agente verifica:

```bash
git status        # confirma que não há conflito de merge pendente
git log -1        # confirma que o commit foi criado corretamente
```

Se `git push` retornar erro de divergência (rejected / non-fast-forward):
1. Executar `git pull --rebase origin <branch>` primeiro
2. Resolver conflito se houver
3. Repetir o push

## Arquivos que nunca devem ser commitados

O agente **nunca** faz `git add` nos seguintes padrões — verificar `.gitignore`:

```
.env
creds.json
*.pyc
__pycache__/
*.egg-info/
.qgis2/
db.sqlite3
```

Se `.gitignore` não existir na raiz, criá-lo com o conteúdo acima antes do
primeiro commit.

## Branch padrão

Usar sempre a branch atual do clone (`git branch --show-current`).
Nunca trocar de branch automaticamente — se a branch não existir no remoto,
usar `git push -u origin <branch>` no primeiro push.

## Confirmação ao usuário

Após cada push bem-sucedido, informar em uma linha:
```
✓ commit + push — <tipo>(<escopo>): <descrição>  [<hash-curto>]
```

Se o usuário escolher **editar a mensagem**, apresentar o campo pré-preenchido
com a sugestão gerada e aplicar a versão editada no commit.

Se o usuário **cancelar**, registrar apenas:
```
⏸ commit cancelado — alterações mantidas no working tree.
```

Em caso de falha no push, reportar o erro completo e aguardar instrução.
Nunca silenciar erros de git.
