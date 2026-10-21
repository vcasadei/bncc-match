# Estrutura da fonte da BNCC (spike)

> Spike da change `curriculum-ingestion` — data: 2026-10-20.

## Fontes

- **Primária**: dados estruturados (JSON) do site oficial
  `basenacionalcomum.mec.gov.br`. O site é uma aplicação web que serve o
  conteúdo da BNCC a partir de dados estruturados (não de parsing de PDF).
- **Referência de validação**: o **PDF oficial** da BNCC (publicado pelo MEC),
  usado apenas pelo script `bncc validate` para conferir os códigos do banco.
  Não faz parte do caminho de ingestão.

## Acesso ao site (limitação encontrada no spike)

- O site responde, mas está atrás de **proteção anti-bot (Cloudflare)**:
  requisições automatizadas recebem `HTTP 403` com a página
  "Just a moment...", mesmo com `User-Agent` de navegador.
- Consequência prática:
  - `bncc ingest --url ...` aponta para o endpoint documentado abaixo, mas
    pode falhar com 403 dependendo do ambiente/proteção ativa.
  - Para ingestão garantida, use **`bncc ingest --file export_bncc.json`**:
    exporte o JSON do site manualmente (o navegador o faz ao navegar pela
    BNCC) e ingira o arquivo local. O CLI suporta os dois modos.
  - Os testes usam fixtures sintéticas que seguem o contrato de payload
    abaixo, sem rede.

## Contrato do payload (JSON)

O payload é um JSON com uma **lista de itens de habilidade/objetivo**, ou um
objeto com a lista sob uma das chaves `items`, `habilidades` ou `data`.
Cada item bruto:

| chave        | tipo            | descrição                                                                 |
|--------------|-----------------|---------------------------------------------------------------------------|
| `code`       | string          | código oficial (ex.: `EF05MA03`) — **obrigatório** (sem código, é rejeitado) |
| `text`       | string          | enunciado da habilidade/objetivo                                          |
| `year`       | int \| null     | ano/série: 1–9 no Fundamental, 13 no Médio, `null` no Infantil            |
| `stage`      | string          | `infantil` \| `fundamental` \| `medio`                                    |
| `path`       | list de [type, name] | cadeia de ancestrais, da raiz (etapa) até o nó-pai da habilidade   |
| `extras`     | object          | metadados da etapa (ex.: `faixa_etaria`, `campo_experiencia`)             |

> O pipeline carimba cada item com `ingest_run` (timestamp do fetch) antes de
> gravar no bronze — campo de proveniência usado pelo load para definir a
> visão atual do gold (o bronze é imutável e acumulativo).

### Tipos de nó (`type`) usados em `path`

`stage`, `area`, `component`, `thematic_unit`, `knowledge_object`. O nó-folha
`skill` é criado no load (a partir do `code`), com o último item do `path`
como pai.

### Exemplos por etapa (mesmos exemplos da fixture de teste)

**Educação Infantil** — faixas etárias → campos de experiência → objetivos
(`area` = faixa etária, `component` = campo de experiência, sem os níveis do
Fundamental):

```json
{
  "code": "EI03A01",
  "text": "Apropriar-se da escuta e da fala como forma de comunicação...",
  "year": null,
  "stage": "infantil",
  "path": [
    ["stage", "Educação Infantil"],
    ["area", "4 a 6 anos"],
    ["component", "Escuta, fala, pensamento e imaginação"]
  ],
  "extras": {
    "faixa_etaria": "4 a 6 anos",
    "campo_experiencia": "Escuta, fala, pensamento e imaginação"
  }
}
```

**Ensino Fundamental (1º–9º)** — área → componente → unidade temática →
objeto de conhecimento → habilidade:

```json
{
  "code": "EF05MA03",
  "text": "Comparar e ordenar números naturais, situando-os na reta numérica.",
  "year": 5,
  "stage": "fundamental",
  "path": [
    ["stage", "Ensino Fundamental"],
    ["area", "Matemática"],
    ["component", "Matemática"],
    ["thematic_unit", "Números"],
    ["knowledge_object", "Números naturais"]
  ],
  "extras": {}
}
```

**Ensino Médio (1ª–3ª)** — área → competência específica → habilidade
(`component` = competência):

```json
{
  "code": "EM13CHS101",
  "text": "Analisar e interpretar as condições de possibilidade e de limitação do trabalho humano...",
  "year": 13,
  "stage": "medio",
  "path": [
    ["stage", "Ensino Médio"],
    ["area", "Ciências Humanas e Sociais Aplicadas"],
    ["component", "Competências específicas de Ciências Humanas e Sociais Aplicadas"]
  ],
  "extras": {}
}
```

## Formato dos códigos oficiais

| etapa      | formato                     | exemplo      |
|------------|-----------------------------|--------------|
| Fundamental | `EF<ano 01-09><disc 2 letras><n 2-3>` | `EF05MA03` |
| Médio       | `EM13<área 2-3 letras><n 2-3>`        | `EM13CHS101` |
| Infantil    | `EI<faixa 01-03><campo A-E><n 2-3>`   | `EI03A01`    |

Regex usada pela validação (`bncc_match.curriculum.validate.CODE_RE`):

```
\b(?:EF\d{2}[A-Z]{2}\d{2,3}|EM13[A-Z]{2,3}\d{2,3}|EI\d{2}[A-Z]\d{2,3})\b
```
