# databricks-lakehouse-anp

Reconstrução do data warehouse de preços de combustíveis da ANP em arquitetura
lakehouse no Databricks, a partir dos mesmos arquivos brutos de uma implementação
existente em PostgreSQL.

A pergunta do projeto não é "dá para fazer no Databricks". É **onde cada
arquitetura ganha, e onde a ferramenta é excesso** — medido, e não afirmado.

O critério de sucesso é objetivo: a camada gold precisa reproduzir as medidas da
implementação de referência. Se reproduzir, a comparação vale. Se não reproduzir,
a divergência vira achado.

---

## Resultado

As duas implementações foram comparadas por consulta federada, lendo o PostgreSQL
de dentro do Databricks como catálogo externo. Não é conferência de números
anotados: é `LEFT ANTI JOIN` e `FULL OUTER JOIN` entre as duas bases.

| Medida | Databricks | PostgreSQL |
|---|---:|---:|
| Observações no fato | 3.029.551 | 3.029.551 |
| Versões de posto (SCD2) | 17.631 | 17.631 |
| Postos distintos | 14.059 | 14.059 |
| Bandeiras canônicas | 56 | 56 |
| Trocas de bandeira | 1.960 | 1.960 |
| Municípios | 462 | 462 |
| Linhas no rollup semanal | 367.142 | 367.142 |

Verificações que fecham a equivalência:

```sql
-- versões que existem em apenas um dos lados: 0 nos dois sentidos
SELECT count(*) FROM anp.silver.dim_station d
LEFT ANTI JOIN neon_anp_catalog.core.dim_station n
    ON n.cnpj = d.reseller_cnpj AND n.valid_from = d.valid_from;

-- semanas do rollup com contagem divergente: 0
WITH db AS (SELECT week_start_date, count(*) AS n FROM anp.gold.weekly_price GROUP BY 1),
     ne AS (SELECT week_start_date, count(*) AS n FROM neon_anp_catalog.analytics.mv_weekly_price GROUP BY 1)
SELECT count(*) FROM db FULL OUTER JOIN ne USING (week_start_date)
WHERE coalesce(db.n, 0) <> coalesce(ne.n, 0);
```

O valor do exercício não está na equivalência em si. Está em que **cada
divergência encontrada no caminho tem causa nomeada** — e nenhuma delas teria
aparecido com uma implementação só.

---

## Arquitetura

```mermaid
flowchart LR
    A["portal da ANP<br/>7 arquivos .zip"] -->|"download + extração"| B["Volume<br/>bronze.landing"]
    B -->|"Auto Loader"| C["bronze.price_raw<br/>3.038.685 linhas"]
    C -->|"tipagem + rejeitos"| D["silver.price_clean"]
    D --> E["silver.brand_alias<br/>silver.dim_brand"]
    D -->|"dedup determinística"| F["silver.price_observation<br/>3.029.551"]
    E --> G["silver.dim_station<br/>SCD2 · 17.631 versões"]
    F --> G
    F --> H["gold.dim_date<br/>gold.dim_city<br/>gold.dim_product"]
    G --> I["gold.fct_price_observation"]
    H --> I
    I --> J["gold.weekly_price<br/>367.142 linhas"]
    C -.-> K["ops.landing_files<br/>ops.silver_rejects<br/>ops.dedup_discarded"]
    D -.-> K
    F -.-> K
    J -.->|"Lakehouse Federation"| L["PostgreSQL<br/>implementação de referência"]
```

**Plataforma.** Databricks Free Edition, compute serverless, Unity Catalog.
Catálogo `anp`, schemas `bronze`, `silver`, `gold` e `ops`. Dois volumes:
`bronze.landing` para os CSVs extraídos e `ops.checkpoints` para o estado do
Auto Loader — separados porque um leitor que enxerga o próprio controle o trata
como dado novo.

**CI/CD.** Databricks Asset Bundles versionados no repositório, com deploy pelo
GitHub Actions a cada push em `main`. O repositório é a fonte da verdade: o
workspace é reconstruível com um comando, o que também protege contra a exclusão
por inatividade do Free Edition.

---

## Estrutura do repositório

```
databricks.yml                 raiz do bundle, variáveis e targets
resources/
  job_setup.yml                catálogo, schemas e volumes
  job_bronze.yml               landing + ingestão
  job_silver.yml               limpeza, bandeiras e dimensão de postos
  job_gold.yml                 dimensões, fato e rollup
src/
  00_setup_catalog.py          namespace do projeto
  10_landing_anp.py            download dos zips e extração para o volume
  20_bronze_autoloader.py      ingestão incremental sem conversão
  30_silver_clean.py           tipagem, normalização e tabela de rejeitos
  35_silver_brand.py           catálogo de bandeiras e mapa de rótulos
  40_silver_station.py         dedup e SCD2 com verificação de invariantes
  50_gold_dimensions.py        calendário, município e produto
  60_gold_fact.py              fato e rollup semanal
.github/workflows/deploy.yml   validate + deploy do bundle
```

---

## Decisões técnicas

**O nome do arquivo carrega o hash do zip de origem.** O Auto Loader controla o
que já leu por caminho de arquivo. A ANP republica o semestre corrente conforme
coleta; gravar com nome fixo faria uma republicação ser ignorada em silêncio.
Com `ca-2026-01__a1b2c3d4.csv`, conteúdo novo é arquivo novo, e as duas versões
convivem no volume para auditoria.

**A bronze não converte nada.** Todas as dezesseis colunas entram como texto. Uma
regra de limpeza aplicada na entrada não pode ser revista sem recarregar a fonte —
e ela sempre precisa ser revista.

**O layout é validado a cada execução, não declarado uma vez.** Os nomes de coluna
vêm do cabeçalho do arquivo e são comparados por posição contra o layout esperado,
ignorando acento e caixa. A validação falha alto em vez de gravar nulo.

**A deduplicação é determinística, não arbitrária.** Vinte linhas excedentes em
três milhões, das quais quatro com preços divergentes. O dado não diz qual está
certa, então a escolha é arbitrária — mas duas execuções precisam produzir o mesmo
resultado. As descartadas ficam em `ops.dedup_discarded`.

**O mapa de bandeiras é curado à mão.** Nenhuma regra automática separa renomeação
de rótulo de troca real (ver Achados). Cada par tem justificativa registrada na
própria tabela, e o notebook recusa mapa com alias encadeado ou rótulo canônico
inexistente.

**As semanas de borda são marcadas, não removidas.** Elas são truncadas por
construção, mas apagar linha na origem tira de quem consome a chance de discordar
do critério.

**Os percentis são exatos, não aproximados.** `percentile_approx` é bem mais
barato, mas comparar mediana aproximada com mediana exata inventaria diferença
onde não há.

**As chaves substitutas são hashes do valor natural, não sequenciais.** A mesma
entrada produz a mesma chave em qualquer execução, o que é o que permite o
anti-join entre bases diferentes.

**A conexão federada é criada pela interface, não por notebook.** A senha do papel
de leitura ficaria em texto no repositório se estivesse num arquivo versionado.

**A tabela nasce sem particionamento ou clustering.** A otimização é medida contra
este estado como linha de base.

---

## Achados

**O Auto Loader casa colunas por nome, não por posição.** Fornecer um schema com
nomes próprios não faz o leitor aplicá-lo posicionalmente: como nenhum nome batia
com o cabeçalho em português, as dezesseis colunas ficaram nulas e o conteúdo
inteiro foi para `_rescued_data` — sem erro, sem aviso. O sintoma inicial parecia
outro, porque a coluna de resgate guarda o caminho do arquivo de origem e por isso
nunca é nula. O diagnóstico veio de `count(DISTINCT _rescued_data)`: sete valores
significariam apenas o caminho; três milhões significavam o conteúdo.

**Os nomes de coluna perdem o acento ao fixar o schema.** O cabeçalho traz
`Município` e o leitor entrega `Municipio`. A validação inicial comparava as
strings exatas e falhava com uma mensagem em que os dois nomes pareciam idênticos
na tela. A comparação passou a dobrar acento e caixa: o que importa é identificar
a coluna, não reproduzir a grafia.

**O arquivo `ca-2025-01` tem 9.114 linhas totalmente em branco.** Todas as
dezesseis colunas nulas, concentradas num único semestre. Não são coletas sem
preço; são linhas vazias de publicação.

**A renomeação de bandeira é um processo, não um evento.** `VIBRA ENERGIA → VIBRA`
aparece em 189 datas distintas ao longo de três anos; `ALESAT → ALE` em 62 datas
ao longo de dois. A hipótese inicial era separar renomeação de troca real pela
simultaneidade — centenas de postos na mesma semana. O dado recusou: a ANP troca o
rótulo posto a posto. Daí o mapa curado.

**A fonte corrige grafia entre arquivos semestrais.** Um endereço com espaço duplo
(`AVENIDA MAJOR  PINHEIRO FROES`) aparece corrigido no semestre seguinte. Com
apenas `trim` aplicado, isso abria versão nova na dimensão de postos sem que nada
tivesse mudado. Sete versões vieram daí, seis delas com vigência iniciando em
janeiro ou julho. A silver passou a colapsar espaços internos.

**Um rollup semanal não pode derivar o mês da data da coleta.** Numa semana que
atravessa a virada de mês, `month_start_date` varia dentro do grupo e a mesma
combinação de semana, município e produto se divide em duas linhas. Vinte e três
semanas afetadas, 4.103 linhas em excesso. O mês passou a ser derivado do início
da semana.

**As semanas de borda ficam em 75,1% da cobertura mediana das demais.** Medido
comparando a soma de observações por semana contra a mediana das semanas internas.

**`infinity` é a decisão certa no PostgreSQL e é o que impede o dado de
atravessar.** A implementação de referência usa `infinity` em `valid_to`, o que
deixa o `daterange` aberto à direita e permite a constraint de exclusão funcionar
sem data sentinela. Ler essa coluna via Lakehouse Federation devolve
`integer overflow`: o valor não tem representação no tipo `DATE` do Spark. Aqui a
versão corrente usa `9999-12-31`, que é feio e portável. Nenhuma das duas é
errada; elas otimizam coisas diferentes, e só a comparação revela o custo.

---

## Onde cada arquitetura difere

| Aspecto | PostgreSQL | Databricks |
|---|---|---|
| Não sobreposição de vigências | `EXCLUDE USING gist` recusa a escrita errada | nenhum mecanismo equivalente: virou verificação após a carga |
| Fim de vigência aberto | `infinity` | `9999-12-31`, por portabilidade |
| Controle do que já foi carregado | catálogo de arquivos com carga idempotente | Auto Loader com checkpoint em volume |
| Papéis de acesso | `owner` e `bi_reader` separados no banco | Unity Catalog |
| Idempotência | migrações numeradas com ledger e checksum | bundle declarativo aplicado por deploy |

O ponto que a comparação torna explícito: no PostgreSQL a garantia de integridade
**é do banco** e a escrita errada não acontece. No Delta a garantia **é da
lógica**, e por isso precisa de verificação explícita. As três invariantes da
`dim_station` — uma versão corrente por posto, vigências contíguas sem
sobreposição, e toda observação encontrando exatamente uma versão — existem
porque a constraint não existe.

---

## Limitações conhecidas

- `TOTALENERGIES → NEXTA`, com 18 postos, não foi classificado como renomeação nem
  como troca real. Não há informação no dado que decida, e tratar como alias
  apagaria um evento possivelmente real.
- Quatro chaves com preços divergentes, todas do mesmo posto em duas datas, são
  resolvidas por critério arbitrário porém estável. O dado não permite decidir.
- A coluna `valid_to` da implementação de referência não pode ser lida via
  Lakehouse Federation enquanto usar `infinity`.
- O Free Edition oferece apenas compute serverless, sem R nem Scala, com cota
  diária. Cargas completas foram executadas uma vez por etapa.
- A dimensão de posto é construída em lote a partir do histórico completo. A
  aplicação incremental por `MERGE` ainda não foi medida contra ela.

---

## Como reproduzir

**Pré-requisitos.** Conta no Databricks Free Edition, Databricks CLI autenticada,
e um repositório no GitHub com os secrets `DATABRICKS_HOST` e `DATABRICKS_TOKEN`.
O token precisa dos escopos `workspace`, `files`, `jobs`, `scim` e `identity` —
verificados como suficientes para o deploy do bundle.

```bash
git clone https://github.com/mrmansini/databricks-lakehouse-anp.git
cd databricks-lakehouse-anp

# ajuste o host do workspace em databricks.yml
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

No workspace, execute os jobs nesta ordem:

1. `setup_catalog` — cria catálogo, schemas e volumes
2. `bronze_ingest` — baixa os sete semestres e carrega a bronze
3. `silver_build` — limpeza, bandeiras e dimensão de postos
4. `gold_build` — dimensões, fato e rollup

O parâmetro `full_refresh` do `bronze_ingest` descarta o checkpoint do Auto Loader
e reprocessa o volume inteiro. O padrão é `false`.

A comparação federada é opcional e exige uma conexão PostgreSQL criada em
**Catalog → External Data → Connections**, apontando para uma instância com a
implementação de referência.

---

Identificadores em inglês, documentação em português.
