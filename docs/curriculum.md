# Currículo BNCC (gold)

A capability `curriculum` mantém a Base Nacional Comum Curricular — da
Educação Infantil ao Ensino Médio — em um SQLite (camada **gold**),
consultável por código oficial de habilidade. Estrutura da fonte:
[`docs/bncc-source-structure.md`](bncc-source-structure.md).

## Tabelas do gold

| tabela            | descrição                                                                 |
|-------------------|---------------------------------------------------------------------------|
| `curriculum_node` | árvore de nós tipados (`stage`, `area`, `component`, `thematic_unit`, `knowledge_object`, `skill`) com `parent_id` (auto-referência); `stage` = etapa, `extras_json` = metadados da etapa |
| `skill`           | dado rico da habilidade: `code` (PK, código oficial = chave de identidade), `node_id` (FK para o nó `skill` na árvore), `text` (enunciado completo), `year` (ano/série; nulo no Infantil), `status` (`active` \| `absent`) |
| `ingestion_run`   | proveniência de cada ingestão: `source_url`, `source_version`, `timestamp` |

- **`status = 'active'`**: habilidade presente na última ingestão.
- **`status = 'absent'`**: habilidade que saiu da fonte em alguma ingestão
  (preservada como histórico; `node_id` é nulo — o nó foi removido da árvore).

## Fluxo bronze → silver → gold

```
fetch (httpx)                payload bruto (JSON)
   │
   ▼
bronze  data/bronze/bncc/<data>/<uuid>.jsonl      (imutável, acumulativo;
   │                                             cada item carimba ingest_run)
   ▼
silver  data/silver/curriculum/<data>-<uuid>.parquet
   │        (validação: code não vazio; normalização: linhas flat,
   │         path/extras serializados em JSON)
   ▼
gold    SQLite: curriculum_node (árvore) + skill (upsert por code)
        + ingestion_run (proveniência)
```

- **Bronze**: cada execução de ingestão escreve um novo JSONL (uuid), nunca
  sobrescreve, e registra o arquivo no manifesto da lake.
- **Silver**: o transform reprocessa todo o histórico do bronze a cada
  execução; cada linha carrega `ingest_run` (timestamp do fetch de origem).
- **Gold**: o load lê a **visão atual** — registros com a maior `ingest_run` —
  reconstrói a árvore em transação e faz **upsert por `code`**:
  - código novo → inserido (`status = 'active'`);
  - código existente → atualizado (`text`, `year`, `node_id`, `active`);
  - código que saiu da fonte → `status = 'absent'`, `node_id = NULL`.
- **Idempotência**: reingestionar a mesma fonte não duplica registros
  (contagens estáveis); bronze acumula arquivos distintos.

## Como (re)executar a ingestão

```bash
# schema (uma vez, ou após mudar a migration):
uv run alembic upgrade head

# da fonte online (pode exigir superação de proteção anti-bot do site):
uv run bncc ingest --url https://basenacionalcomum.mec.gov.br/api/habilidades --version 2026-10

# de um export local do JSON (recomendado quando o site bloqueia bots):
uv run bncc ingest --file export_bncc.json --version 2026-10
```

Saída:

```
bronze: data/bronze/bncc/2026-10-20/<uuid>.jsonl
silver: 3 linhas (0 rejeitadas)
gold: 31 nós, 3 habilidades (0 marcadas ausentes), run_id=1
```

Variáveis de ambiente:

| variável          | padrão                    | descrição                  |
|-------------------|---------------------------|----------------------------|
| `BNCC_LAKE_ROOT`  | `data`                    | raiz da data lake          |
| `BNCC_DB_PATH`    | `data/gold/curriculum.db` | arquivo SQLite do gold     |

## Como consultar a árvore por código

```bash
uv run bncc get EF05MA03
```

Saída:

```
código: EF05MA03
etapa: fundamental | ano: 5 | status: active
hierarquia (raiz → nó-pai):
  stage: Ensino Fundamental
  area: Matemática
  component: Matemática
  thematic_unit: Números
  knowledge_object: Números naturais
texto: Comparar e ordenar números naturais, situando-os na reta numérica.
```

Em Python (API interna usada pelas próximas capabilities):

```python
from bncc_match.curriculum import get_skill, list_skill_codes

skill = get_skill("EF05MA03")   # dict com code/text/year/stage/status/ancestors
                                  # (None se o código não existir — sem exceção)
list_skill_codes()              # códigos ativos, em ordem alfabética
```

`ancestors` é a cadeia de ancestrais em ordem raiz → nó-pai; cada entrada tem
`type`, `name`, `stage` e `extras` (ex.: faixa etária/campo no Infantil).

## Validação contra o PDF oficial

O PDF oficial é a referência canônica de integridade: a validação extrai os
códigos de habilidade do PDF e compara com os códigos **ativos** do gold,
gerando um relatório de divergências.

1. **Baixe o PDF oficial** da BNCC (publicação do MEC — o PDF pode ser
   baixado na própria página da BNCC em `basenacionalcomum.mec.gov.br`;
   arquivo de referência: `BNCC_EI_EF_110518_versaofinal_site.pdf`).
2. **Rode a validação**:

   ```bash
   uv run bncc validate caminho/para/bncc.pdf
   ```

3. **Interprete o relatório**:

   ```
   códigos no PDF: 1234 | no gold: 1234
   OK: sem divergências entre PDF e gold
   ```

   - `somente no PDF (n): ...` — códigos que existem no PDF oficial mas
     **faltam no banco** (ingestão incompleta/antiga);
   - `somente no gold (n): ...` — códigos no banco que **não estão no PDF**
     (dados errados ou PDF de versão diferente).

   Códigos de saída: `0` = sem divergências, `2` = com divergências,
   `1` = erro (ex.: PDF inexistente).

> A validação é offline e não altera o banco. Os padrões de código
> (`EF..`, `EM13..`, `EI..`) estão documentados em
> [`docs/bncc-source-structure.md`](bncc-source-structure.md).
