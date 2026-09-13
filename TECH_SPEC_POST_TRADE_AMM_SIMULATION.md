# ТЗ: модуль последовательной post-trade AMM simulation

Версия 1.0 · 11 сентября 2026 года · Crypto market-data lab.

Документ предназначен агенту-исполнителю. Это задание на будущую реализацию, а не описание уже работающего модуля. Язык документа — русский; код, публичные идентификаторы, docstrings и диагностические reason codes — английские.

## 1. Результат и границы задания

Реализовать детерминированный read-only вычислитель, который получает версионированное состояние AMM и упорядоченный план swaps, вычисляет точные движения токенов и после каждого шага создаёт виртуальное состояние для следующего шага. Повторное использование одного пула в пути обязано потреблять уже изменённую ликвидность.

Основной сценарий: `stable → base → stable` в одном пуле; второй swap использует post-state первого. Дополнительные сценарии: последовательные пути до четырёх swaps через несколько известных пулов, повторное посещение пула, точный размер для DEX/perp-хеджа и отдельная переоценка выхода на зафиксированное количество позиции.

Модуль возвращает amounts, fees, изменения состояния, состояние остатков, ограничения и воспроизводимые доказательства. Он не рассчитывает funding, не открывает позиции сам, не подменяет Portfolio/Position Engine и не объявляет результат реальным исполнением.

Обязательные границы:

- Никаких кошельков, приватных ключей, подписей, построения/отправки транзакций, orders, transfers, approve или flash loans.
- Никаких новых per-strategy collectors. Использовать существующий Python supervisor и его TypeScript worker.
- Изолировать transport/state acquisition от чистого расчёта. Во время самого перехода состояния запрещены RPC/HTTP/WS, файловый I/O и чтение текущего времени.
- Не перестраивать весь репозиторий под дерево из общего ТЗ. Добавить компактные модули рядом с существующими.
- Изменять только код и конфигурационные шаблоны, необходимые этому модулю. Не менять `config/solana-rpc.local.toml`, не запускать и не перезапускать пользовательский непрерывный сканер в рамках реализации без отдельной команды пользователя.
- Сохранять чужие изменения и исторические артефакты. Не переписывать старые candidate records под новую семантику.

### 1.1. Объём поставки по протоколам

| Этап | Обязательная поставка | Что не считается её завершением |
| --- | --- | --- |
| A: ядро | Synthetic CPMM, snapshot/overlay, path executor, exact-in/out контракты, fixtures | Только функция формулы без версий и post-state |
| B: первый рабочий срез | Raydium CPMM и классический Orca Whirlpool; Broker, worker, shadow DEX/perp integration | SDK single-swap quote с переименованным флагом |
| C: весь модуль | Raydium CLMM, Meteora DLMM; audit Raydium AMM v4 и поддержка проверенного subset; совместные multi-pool пути | Объявление всех вариантов протокола supported по одному happy-path |

A и B — промежуточные поставки. Полное задание завершено после C, тестов и документации. Для каждой семьи обязательны положительные fixtures поддерживаемого варианта и отрицательные fixtures неподдерживаемых вариантов. Если отсутствуют достаточные исходники/данные для точного перехода, конкретный вариант остаётся `unsupported` с доказанной причиной; это не разрешение молча исключить всю обязательную семью и объявить полную готовность.

EVM Uniswap, TON/STОN.fi, Omniston, Jupiter/Raydium aggregator split routes, RFQ, Drift DLOB/vAMM, adaptive/hook расширения без проверенной модели не входят в реализацию адаптеров этого задания. Для них нужен честный capability/fallback. Интерфейсы не должны препятствовать последующему расширению.

## 2. Связь с общим планом

Нормативная основа: `TECH_SPEC_STRATEGY_ENGINE.md`, прежде всего §6–9, §11–14, §18–20.

| Требования общего ТЗ | Ответственность данного модуля |
| --- | --- |
| M1, DATA-01/04, TIME-01/02 | Полная identity assets/pools; immutable snapshot и account version vector; реальная свежесть зависимостей |
| M2, EXE-01/02 | Точный swap и `apply_virtual_fill`; стоимость net quantity; post-state, fee breakdown |
| M2, SIZE-01/02 | Exact quantity и явный residual; запрет масштабирования старого exact quote |
| M2, QTE-01–03 | Local backend через Broker, cache/dedup/deadline, shared refresh budget |
| M3, S03/S04 | Исполнитель уже выбранного последовательного пути; поиск маршрутов и optimizer остаются снаружи |
| M4, S06/S07 | Поставка DEX entry/exit evidence; funding, borrow, margin и lifecycle не реализуются здесь |
| M5 | Минимальные доказательства и replay расчёта; глобальные reservations/portfolio allocator остаются снаружи |
| T09–12, T14, T17, T22–30, T32–35, T38 | Приёмочные тесты, раскрытые в §15 |

Наличие M1/M2-классов в коде не означает завершение всех M1/M2 требований. `StateView.consistent` не является доказательством атомарного on-chain snapshot. Pairing котировок не является post-trade simulation. Успех этого задания не означает готовность M4–M6.

Уточнение T09 обязательно включить в документацию реализации: при reserves `1000/1000` **в raw units**, fee=0, input=100, floor output, последовательность даёт `100 → 90 → 99`. Значение 100 относится к непрерывной арифметике без округления. Не менять формулу ради буквального получения 100.

## 3. Проверенное состояние репозитория

Аудит исходников и локальная регрессия выполнены 11.09.2026. Python: `>=3.12,<3.15`; Node: `>=22`; TypeScript strict/NodeNext/ES2022. Текущие проверки: 181 Python-тест прошёл; `npm run check` и `npm test` worker прошли. Это baseline, который исполнитель повторяет на своей исходной ревизии.

Версии из `workers/solana-quote-worker/package-lock.json`: Raydium SDK `0.2.63-alpha`, Orca SDK `0.22.0`, Meteora DLMM `1.9.14`, Solana web3.js `1.98.4`. В package.json есть `latest`; установка/обновление не должна незаметно сменить lockfile. Локальный SDK и актуальный upstream могут различаться; подтверждать конкретную пару SDK/program semantics.

### 3.1. Карта проекта и точки изменения

| Файл/компонент | Текущее поведение | Требуемое изменение |
| --- | --- | --- |
| `scanner.py` | Единый длительный запуск, manifest/status | Только безопасная конфигурация флага/диагностика при необходимости |
| `unified_market_data.py::build_unified_market_data_scanner` | Создаёт `QuoteBroker(backends={})`; polling seeds cache; `_local_quote_source` не подключён как backend | Инъекция одного local simulation backend и result consumer |
| `realtime_scanner.py`, `versioned_market_state.py` | Envelopes, epoch/TTL, state versions, bounded bus | Связь snapshot tokens с версиями/инвалидацией; не публиковать виртуальные pools как market updates |
| `solana_realtime_scanner.py::RaydiumLocalQuoteStateSource` | Жизненный цикл worker, pool notices, `quote_exact_input` | Typed snapshot/path операции и capability notices |
| `solana_quote_worker.py::RaydiumLocalQuoteWorker` | JSONL stdin/stdout, pending quote futures, bounded events | Typed path futures, timeout/cancel, worker generation, line-size limits |
| `workers/.../protocol.ts`, `worker.ts` | configure/quote_request/shutdown; до 32 in-flight quote promises | Версионированные snapshot/path messages, bounded scheduler, совместимость старого протокола |
| `raydiumStandard.ts` | SDK CPMM, отдельная AMM v4 формула; mutable state/vault caches | Извлечение immutable snapshots; отдельная проверенная transition model |
| `raydiumClmm.ts` | `PoolUtils.computeAmountOutFormat`, ticks/bitmap/chain context | Offline transition с реальным updated state и полными зависимостями |
| `orcaWhirlpool.ts` | `swapQuoteWithParams`, directional tick arrays/token extensions | Offline переход по ticks и согласованный mutable-state projection |
| `meteoraDlmm.ts` | `swapQuote`, bins и LB-pair cache | Обновление bin balances, active ID и fee-related state |
| `quote_broker.py` | Typed QuoteKey/Result; dedup/cache/deadline; `estimate_local_path` — alias `get_quote` | Реальный отдельный path contract; никакой silent remote fallback для sequential запроса |
| `execution_cost.py` | Чистый L2 cost engine и CostComponent | Переиспользовать соглашения о fees/movements; не превращать AMM в L2 |
| `solana_route_evaluator.py` | Single-pool DEX + CEX depth, собственный вызов quote source | При включённой новой ветке использовать Broker; old mode оставить рабочим |
| `unified_perp_analyzer.py::_evaluate_dex_perp_paired_exact_quote` | Две independent pre-trade quotes; `candidate_eligible=False` | Отдельная sequential-result ветка, прежнюю не переименовывать |
| `exact_quote_pair_cache.py`, `polling_quote_sources.py` | Exact pair identity/epoch, remote observations | Не считать их входом для state mutation без полного snapshot/route proof |
| `strategy_quality.py` | `sequential_simulation` выставляется по boolean | Проверять достоверный typed evidence, а не один boolean |
| `quantity_lattice.py`, `contract_models.py`, `funding_model.py` | Существующие quantity/perp/funding проверки | Переиспользовать; не дублировать внутри AMM |
| `cex_*`, `perp_venue_feeds.py`, `live_*`, архивные recorders/monitors | Сбор, старые сценарии/анализ и offline данные | Сохранять API; не запускать дополнительные collectors |
| `solana_pool_registry.py`, discovery TS | Каталог/идентификация пулов | Использовать identity/owner; registry не является ценовым snapshot |
| `account_fee_audit.py`, `clock_*`, `jupiter_gated_verifier.py` | Fees/time/редкая внешняя проверка | Учитывать контракты; без нового account доступа и verification на каждом tick |

Все Python-пути таблицы находятся в `src/market_data_lab/`, TS — в `workers/solana-quote-worker/src/`, кроме явно указанных root files. Просмотреть соответствующие `tests/test_*.py` и worker `test/protocol.test.ts` до изменений.

### 3.2. Известные риски текущей реализации

1. Worker cache содержит mutable BN/SDK objects. `frozen dataclass`, `Readonly<T>`, `Object.freeze` и shallow spread сами по себе не обеспечивают глубокую изоляцию.
2. Pool notices содержат summaries, а не весь набор accounts/ticks/bins/fees. По ним нельзя восстановить полный transition.
3. `minimum_state_slot` означает нижнюю границу, а не pin конкретного состояния. Quote functions могут делать await refresh; после await живая версия могла измениться.
4. Timestamp всего tick cache не доказывает свежесть каждого account. Обновление одного массива не должно омолаживать остальные.
5. В `raydiumStandard.ts::quoteCpmm` `creatorFeeOnInput` сейчас вычисляется без направления: `feeOn === 0 || feeOn === 2`. Для нового engine нужна проверка по `(fee_on, direction)`, а не копирование выражения.
6. SDK `CurveCalculator.swapBaseInput` возвращает `newInputVaultAmount = reserveIn + inputLessFees`. Название не доказывает, что это реальный vault balance или reserve после начисления всех fee counters. Использовать contract accounting, описанное в §8.
7. AMM v4 сейчас использует `vault - needTakePnl`; нельзя автоматически объявить эту проекцию полной моделью OpenBook-интегрированного пула.
8. `QuoteResult` не хранит post-state body; его `status` ограничен собственным Literal. Новый result должен иметь свою schema, а mapping в legacy — явный.
9. Python reader использует bounded stream; большой snapshot нельзя внезапно отправить одной неограниченной JSONL-строкой.
10. Качество симуляции не должно зависеть от того, насколько положительным оказался PnL.

### 3.3. Наблюдения длительного запуска

Сохранённый run: `data/live/scanner/all-markets-20260909-213627`. Последний status: `2026-09-10T16:21:43Z`, длительность около 67 516 секунд, supervisor restarts=0. На момент подготовки документа tmux-сессия отсутствует; status `running` является оставшимся файлом, а не подтверждением активного процесса.

Сохранённые counts: около 1.18 млн cycle evaluations; 166.5 млн strategy evaluations в perp-анализаторе; 397 matched DEX pairs; около 12 млн exact quantity mismatches; 21 matured terminal candidate. Это повторные вычисления, не независимые сделки. Среди верхних cycle aggregates есть аномальные величины около 54 000 bps по BONK — их причина здесь не установлена, они не являются golden truth или доказанным арбитражем.

Следствия для модуля: обязательны дедупликация, exact identity/raw amounts, selective verification и fail-closed evidence. Нельзя тестировать AMM на условии «новая прибыль совпала со старым максимумом». Старые данные не содержат полного account replay; восстановить по одним stats точное состояние AMM невозможно.

## 4. Экономическая семантика

Различать четыре операции:

1. `quote_on_observed_state`: отдельный swap на наблюдённом snapshot.
2. `simulate_sequential_path`: swaps на одной ветке виртуального состояния с переносом всех собственных изменений.
3. `simulate_frozen_market_roundtrip`: immediate вход/выход, отсутствие внешних trades, LP changes и течения времени между шагами задано сценарием.
4. `revalue_position_exit`: новое observed state, количество из фактов позиции. Виртуальный вход нельзя второй раз применять к свежему observed snapshot.

Post-trade означает counterfactual состояние после наших виртуальных swaps. Оно не является наблюдённым блокчейн-состоянием. Оно не резервирует ликвидность, не доказывает atomic inclusion, не гарантирует доступность same-block execution и не предсказывает рынок через час.

Для immediate roundtrip время chain context фиксировано. При исследовании удержания требуется явно именованный сценарий внешнего рынка/времени; funding рассчитывается снаружи. Просто прибавить часовой funding к immediate post-state и назвать это доказанным часовым PnL запрещено.

## 5. Архитектура и ответственность

```text
Existing collectors / existing worker account caches
                 ↓ validate + capture immutable dependencies
             SnapshotRegistry (worker-owned, bounded)
                 ↓ snapshot token + provenance notice
Python QuoteBroker → local path backend → existing worker JSONL dispatcher
                                         ↓
                          pure simulatePath(snapshot, request)
                                         ↓
                        protocol transition adapters + overlay
                                         ↓
                          typed result + evidence + state diffs
                 ↓ validate versions, freshness, exact quantity
            cycle / perp consumers + bounded reporting
```

Чистая AMM-арифметика выполняется в TypeScript рядом с существующими SDK. Python владеет orchestration, TTL, consumer policy и общим quote lifecycle. Не заводить две production реализации CPMM в Python и TS; независимый Python reference допустим только в тестах.

Предлагаемые новые файлы:

```text
src/market_data_lab/
  amm_simulation.py                 # frozen Python DTO, validators, mapping
  amm_simulation_backend.py         # Broker ↔ existing worker bridge
workers/solana-quote-worker/src/simulation/
  types.ts                         # domain unions/DTO
  codec.ts                         # validation, canonical encoding/hashes
  snapshots.ts                     # capture, registry, capabilities
  overlay.ts                       # private branch/state isolation
  path.ts                          # pure ordered path executor
  syntheticCpmm.ts                 # offline synthetic reference model
  raydiumCpmm.ts
  raydiumClmm.ts
  orcaWhirlpool.ts
  meteoraDlmm.ts
  raydiumAmmV4.ts                   # audited restricted subset
workers/solana-quote-worker/test/simulation/
tests/test_amm_simulation.py
tests/test_amm_simulation_backend.py
tests/fixtures/amm_simulation/      # shared versioned decimal-string fixtures
tools/benchmark-amm-simulation.ts   # optional bounded offline benchmark entry
```

Файлы можно объединять по смыслу до устойчивых контрактов. Не создавать `execution_cost/` одновременно с одноимённым существующим `.py` и не перемещать все модули ради красоты дерева.

## 6. Контракты данных

Публичные поля обязательны, если не помечены optional. Коллекции immutable; сериализация версионирована. Python DTO — `dataclass(frozen=True, slots=True)`; TS — discriminated unions/readonly interfaces. Вложенные структуры должны реально принадлежать snapshot или быть immutable copies.

### 6.1. Identity

`AssetRef`: `asset_id`, `chain_namespace`, `chain_id`, `address_or_native_id`, `decimals`, `token_program`, `token_extensions_fingerprint`, `spec_version`. `asset_id` канонический, не ticker. Проверка согласованности asset_id и metadata обязательна. Существующие строковые Broker IDs адаптировать в одном месте.

`PoolRef`: `chain_namespace`, `chain_id`, `program_id`, `pool_address`, `protocol`, `protocol_revision`, `asset_0_id`, `asset_1_id`, `pool_spec_version`. Ключ общей ликвидности — реальный pool/account identity, не provider alias/route label. Один pool под двумя aliases обязан иметь одно состояние.

`AccountVersion`: `address`, `owner_program_id`, `data_hash`, `local_revision`, `context_slot`, optional `write_version`, `received_realtime_ns`, `received_monotonic_ns`, `validated_realtime_ns`, `validated_monotonic_ns`, `read_batch_id`. Времена первой публикации и последней реальной перепроверки различаются. Отсутствующие chain metadata не выдумывать.

Base58 mint/pool addresses регистрозависимы: не применять `.lower()`/`.upper()` для канонизации. Synthetic identities допускаются только в offline fixture registry и не должны проходить production owner/address validation.

### 6.2. Snapshot и виртуальное состояние

`AmmSnapshot`:

- `schema_version`, `snapshot_id`, `snapshot_hash`, `worker_generation`, `source_epoch`, `boot_id`, `model_version`, `sdk_versions`;
- canonical `pool_refs`, точный `dependency_vector`, immutable protocol-specific pool bodies;
- `chain_context`: slot/commitment, chain timestamp/epoch при необходимости, источник времени; optional block hash;
- `chain_consistency`: `validated_multi_account_snapshot | slot_window_estimate | unknown`; расширять до `pinned_consistent_snapshot` только при реальном доказательстве такого чтения;
- `capture_started_*`, `capture_completed_*`, `state_valid_until_monotonic_ns`, `integrity`, `capability_refs`.

`snapshot_hash` — canonical domain content + versions + model/context, но не случайный request ID и не время сериализации. Hash определяется одним shared codec, fixture проверяет одинаковый результат Python/TS. Использовать SHA-256, UTF-8, sorted object keys, amounts как десятичные строки, explicit null policy, порядок swaps значим. Метаданные для transport и timings отделить от economic content hash.

`VirtualStateRef`: `root_snapshot_id`, `root_snapshot_hash`, `branch_id`, `step_index`, `parent_state_hash`, `state_hash`. Виртуальные версии не занимают observed slots и не публикуются в live store. `state_hash` отражает достаточное состояние для последующих поддержанных swaps, а не только amountOut.

Live registry хранит immutable roots. Внутри одного path используется copy-on-write overlay изменённых pools/accounts. После timeout/failure overlay уничтожается. Межзапросные mutable sessions в первой версии не требуются: весь путь исполняется одним запросом; передача state-after между отдельными запросами допускается только через полный replayable bundle или документированный immutable token с bounded retention.

### 6.3. Swap leg

`SwapLeg` содержит:

- `leg_id`, `pool_ref`, `input_asset_id`, `output_asset_id`, `mode=exact_in|exact_out`;
- discriminated `amount_source`: `literal` с `amount_raw` либо `previous_output` с `previous_leg_id`;
- `previous_output` означает **net output получателя** непосредственно предыдущей ноги, только exact-in; нельзя подставлять expected/min output, gross vault output или вывести весь баланс актива;
- optional `sqrt_price_limit_raw`/protocol limit, `minimum_net_output_raw` для exact-in либо `maximum_gross_input_raw` для exact-out;
- `fee_policy_fingerprint`, `constraints_fingerprint`.

Для первой версии explicit linear order; любые split/merge DAGs отклонять `unsupported_route_shape`. Все assets связаны точно, соседние ноги принадлежат одной chain/execution domain. Wrap/unwrap должен быть отдельной подтверждённой операцией; скрытого SOL↔WSOL ребра нет.

### 6.4. Path request

`AmmPathRequest`: `schema_version=1`, `request_id`, `reason`, `priority`, `snapshot_ref`, `legs`, `initial_balances` (raw по asset/location), `scenario_id`, `scenario_kind=frozen_market`, `chain_context`, `required_consistency`, `limits`, `execution_policy=require_complete`, optional `candidate_ids`, `position_ids`.

Python API дополнительно получает `deadline_monotonic_ns`. В JSONL передавать оставшийся `timeout_budget_ms`, рассчитанный непосредственно перед записью; worker заводит свой local deadline. Не сравнивать Python monotonic epoch с Node hrtime epoch. Родитель остаётся авторитетным по истечению срока и не принимает опоздавший результат.

В v1 `limits`: число ног, ticks/bins/steps, bytes, computation budget. Начальные balances являются сценарием, не подтверждением кошелька. До каждой ноги проверять доступные средства; отсутствие промежуточного актива не компенсируется виртуальным займом.

### 6.5. Swap result и path result

`AmmSwapResult`:

- `status`, `reason`, `complete`, `leg_id`, protocol/model version;
- `requested_amount_raw`, `actual_gross_input_raw`, `input_received_by_pool_raw`, `input_used_for_curve_raw`, `gross_pool_output_raw`, `actual_net_output_raw`, `unconsumed_input_raw`;
- `fees`, `token_movements`, `state_before_ref`, `state_after_ref`, `state_diff`;
- `consumed_accounts`, `ticks_crossed`/`bins_visited`, coverage/limit diagnostics, optional display price impact.

`FeeComponent`: `kind`, `asset_id`, `amount_raw`, `payer`, `recipient_bucket`, `included_in_amount`, `parent_fee_id` optional, `source_model`. Разделять trade/LP/protocol/fund/creator/transfer fee. Protocol/fund доли trade fee не суммировать второй раз как дополнительные затраты трейдера. Gas/priority fee не являются pool fee.

`TokenMovement`: asset/location, signed delta raw, category, leg reference. Для сохранения токенов учитывать trader, vaults и внешние fee/withheld buckets; внутренний fee counter внутри vault не является дополнительным кошельком, иначе баланс задвоится.

`AmmPathResult`: request/snapshot refs; `status`, `reason`, `complete`, `failed_leg_id`; leg results; initial/final virtual state refs; net movements/remaining balances; initial funding requirements; dependency versions; chain consistency; `state_after_scope=swap_execution_projection`; `exactness=protocol_integer`; `firmness=simulated`; timing diagnostics; `execution_ready=false`; evidence hash/reference.

В результате нет `realized_pnl`. Выходной settlement amount допускается для complete path; при failure нельзя отдавать output префикса как итог полного пути.

`state_after_scope=swap_execution_projection` означает достаточный state для повторных swaps поддержанного варианта. Это не byte-identical chain state всего протокола: fee-growth/oracle/reward fields, не влияющие на дальнейшие поддержанные swaps, могут быть вне проекции только с явным перечнем. Если поле влияет на fee/validity/amount, его исключать нельзя. LP accounting и произвольная serialised transaction simulation здесь не обещаются.

### 6.6. Статусы

Closed Literal/union: `ok`, `invalid_request`, `unsupported_protocol`, `unsupported_pool_variant`, `unsupported_token_extension`, `unsupported_route_shape`, `asset_mismatch`, `state_unavailable`, `state_stale`, `state_version_mismatch`, `state_inconsistent`, `insufficient_input_balance`, `insufficient_liquidity`, `insufficient_state_coverage`, `exact_size_unavailable`, `price_limit_reached`, `slippage_limit_exceeded`, `arithmetic_error`, `work_limit_exceeded`, `deadline_exceeded`, `worker_unavailable`, `worker_restarted`, `cancelled`, `internal_error`.

Различать недостаток ликвидности и неизвестность за границей массива. Expected market failure возвращается typed result; повреждённая transport schema не превращается в market no-liquidity. Legacy `QuoteStatus` mapping задокументировать: не передавать неподдерживаемый Literal в конструктор. Всегда сохранять исходный AMM reason в evidence.

## 7. Snapshot consistency, свежесть и изоляция

1. Собрать список dependencies **до** capture; pool, vaults, fee config, mint/program extensions, ticks/bins/bitmap, необходимые oracle/epoch/time fields.
2. Скопировать нужные decoded данные синхронно в одном JS event-loop сегменте без await; deep-copy BN/Buffer/arrays или canonical immutable encoding. Если нужен I/O, завершить acquisition и затем захватить весь bundle заново.
3. Проверять owner/layout/mints/decimals/program IDs, pool membership всех массивов, integrity и supported variant.
4. Реальный `getMultipleAccounts...AndContext` batch сохранить как один read_batch. `minContextSlot` — нижняя граница, не выбор исторического slot. Несколько разных batches или одинаковые WS slot numbers не дают автоматически одно атомарное состояние. [Solana RPC](https://solana.com/docs/rpc/http/getmultipleaccounts)
5. Для strict multi-pool verification требовать подтверждённый совместный capture зависимостей. При невозможности — диагностический `slot_window_estimate` и отказ в strict eligibility. Максимальный gap сам по себе не превращает estimate в pinned snapshot.
6. Per-account revision меняется при изменении данных; validated timestamps обновляются только после действительной перепроверки. Pool update, ticks/bins, fees, token extensions, epoch rules и worker reconnect инвалидируют dependent snapshots/results.
7. Snapshot, полученный до reconnect, не использовать в live verification после reconnect. Replay historical snapshot разрешён явно в offline mode без fake freshness.
8. Два clock возраста проверяются в Python, как существующие QuoteResult; boot mismatch и UTC jump/suspend не продлевают validity. Worker сообщает возраст зависимостей/время capture, Python отдельно отмечает receipt. Поздняя IPC доставка не делает старые accounts свежими.
9. Во время расчёта новые market updates могут идти параллельно в collector. Расчёт остаётся детерминированным на своём snapshot; перед повышением live quality проверяются актуальные dependency versions и TTL. Старый результат можно сохранить как historical, но не выдать за текущий.
10. Ни одна ветка не пишет свой state-after в cache collector, subscription objects, shared SDK instance или `VersionedMarketState.put`.

## 8. Арифметика и protocol adapters

### 8.1. Общие правила

- Python amounts — `int`; TS — `bigint` либо BN в одном явно выбранном adapter boundary. JSON raw amounts — только canonical base-10 strings.
- Запрещены float/JS Number для reserves, liquidity, sqrt price fixed-point, amounts, fees и PnL. Number допустим для проверенных small indexes, lengths, millis и decimals metadata.
- Raw input положителен. Bool не является валидным int. Reject sign, whitespace, exponent, NaN/Infinity и leading-zero ambiguity. Decimal display conversions проверяют finite, decimals и round-trip raw.
- Intermediate widths/overflow semantics соответствуют протоколу. Arbitrary precision языка не даёт права принимать результат, который контракт отверг бы из-за checked overflow.
- Decimal применяется на Python boundary для display/сочетания с CEX cost engine; не использовать default Decimal precision для on-chain mul/div.
- Round direction определяется точным contract operation, а не общим правилом «везде округлить вниз». Отдельно тестировать fee ceil/floor, fixed-point conversion и exact-out input ceil.
- Price impact уже внутри swap amount. Slippage threshold — проверка допустимости, а не ещё одно вычитание процента. Нарушение limit даёт отказ whole path в complete mode.

### 8.2. Synthetic CPMM: независимый математический oracle

Модель `synthetic_cpmm_v1`: обычные токены, LP fee остаётся в reserve, нет внешних fee counters. Reserves X/Y — raw liquidity, input a в X, fee f=ceil(a*n/d), effective e=a-f.

<tg-math-block>
b=\left\lfloor\frac{Y e}{X+e}\right\rfloor,\qquad X'=X+a,\qquad Y'=Y-b.
</tg-math-block>

Проверки: X,Y,a>0; 0≤n<d; e>0; 0<b<Y; trader/vault token conservation; invariant product не уменьшается для этой модели.

Golden case T09 с n=0:

| Действие | Input raw | Output raw | State X/Y после |
| --- | --- | --- | --- |
| A→B на S0 | 100 | 90 | 1100 / 910 |
| B→A на S1 | 90 | 99 | 1001 / 1000 |
| B→A на независимом S0 — контроль | 90 | 82 | Другой, независимый сценарий |

Golden fee case n=1,d=100: первая fee=1, output=90, S1=1100/910; обратная fee=1, output=97, S2=1003/1000. Итог trader delta A=-3, B=0. LP fees включены в reserves, не вычитать их второй раз из результата.

Synthetic exact-out: e=ceil(X*b/(Y-b)), затем минимальное gross a, для которого a-ceil(a*n/d)≥e. При нулевой fee b=90 требует a=99; a=98 недостаточно. Exact-out transition забирает ровно запрошенный output по своему контракту; если bounded inversion exact-in перескакивает output, это не native exact-out. Не выдавать `at_least` за `exact`.

Эта модель — oracle тестов, не универсальная замена Raydium/Orca/Meteora.

### 8.3. Raydium CPMM

Проверить pool/config/vault/token program/status/open time и конкретную protocol revision. Snapshot содержит реальные vault balances и accrued protocol/fund/creator counters отдельно от effective trading reserves; сохранить feeOn/direction/config.

Creator fee input/output определяется парой настройки и направления. Protocol/fund fees — доли trade fee. Изменения vaults и обязательств по комиссиям формируются отдельно; effective reserves для следующего swap выводятся повторно из обновлённого состояния. Не использовать `reserve += inputLessFees` как полную модель LP reserve и не обнулять accrued counters. Семантику сверить с [pool state](https://github.com/raydium-io/raydium-cp-swap/blob/master/programs/cp-swap/src/states/pool.rs) и [contract calculator](https://github.com/raydium-io/raydium-cp-swap/blob/master/programs/cp-swap/src/curve/calculator.rs).

Использовать SDK math, где она соответствует закреплённой версии. Обязательно проследить contract swap handler до фактических transfers/counter updates. SDK quote equality проверяет amount, но не доказывает правильность полного post-state. Fixture должен содержать before/after vaults, fee counters и второй reverse swap. Покрыть BothToken/OnlyToken0/OnlyToken1 в обоих направлениях.

Первый supported subset — обычный SPL Token без transfer extensions. Token-2022 variant возвращает explicit unsupported до отдельного tested adapter. Не подставлять дефолтную fee при отсутствии config.

### 8.4. Orca Whirlpool

Использовать sqrtPrice fixed-point, liquidity, current tick, tick spacing, initialized tick arrays с направлением, fee/protocol fee и необходимые token/oracle context. Проход по ticks обязан вернуть next sqrtPrice/tick/liquidity, consumed amounts, fees и достаточные изменения projection для следующего swap. В обеих сторонах границы tick корректно применять signed liquidityNet и соглашение current tick.

В v1 supported subset — классический Whirlpool без непроверенных adaptive-fee/token-extension вариантов. Наличие новой oracle/adaptive policy не разрешается молча игнорировать. Нет arrays/unknown next initialized boundary — `insufficient_state_coverage`, не продолжение по последней цене. [Tick arrays](https://docs.orca.so/developers/architecture/tick-arrays), [официальный repository](https://github.com/orca-so/whirlpools).

Можно использовать низкоуровневые SDK primitives; если публичный quote API не возвращает sufficient transition, реализовать обёртку/порт соответствующего math с лицензией и versioned oracle fixtures. Простая подмена только estimatedEndSqrtPrice недостаточна, если пропущены изменения liquidity/fees/validity context.

### 8.5. Raydium CLMM

Отдельный adapter: не reuse Orca transition только из-за похожих sqrt-price формул. Snapshot включает pool, fee config, ticks, bitmap extension, liquidity, timestamp/epoch/token metadata. Точные правила tick crossing, fee calculation, лимитов и rounding сверить с [официальным program repository](https://github.com/raydium-io/raydium-clmm).

`computeAmountOutFormat` текущего worker используется как single-swap comparison, не как доказательство изменённого состояния. `allTrade=false` не становится successful complete quote. Для потреблённого префикса нужны actual consumed amounts; если API их не даёт, возвращать failure без придуманного partial fill. Epoch/time-зависимые правила фиксировать во входном context. Запрещён await скрытого chainContext refresh внутри pure simulation.

### 8.6. Meteora DLMM

Snapshot: LB pair state, active ID, binStep, bin arrays и bitmap dependencies, token/reserve metadata, static и variable fee parameters, timestamp/reference/volatility context необходимых алгоритмов. После swap обновить token amounts затронутых bins, active ID, protocol/LP accounting и fee-related state, влияющий на следующий swap.

Не заменять DLMM CPMM-формулой и не считать `swapQuote` повторно на неизменном SDK object. Не считать fee постоянной на всём пути без подтверждения. SDK предоставляет quote helpers и volatility operations; адаптировать закреплённую версию и указать предел независимой проверки. [SDK](https://github.com/MeteoraAg/dlmm-sdk), [официальная справка](https://github.com/MeteoraAg/docs/blob/main/developer-guides/dlmm/typescript-sdk/reference.mdx).

Если независимая программа-источник недоступна, fixture из отдельной проверенной реализации/предоставленного replay с after-state допустим с честным provenance; повторный вызов того же helper независимым oracle не является. Невозможность подтвердить transition удерживает соответствующий capability в unknown, а не в verified.

### 8.7. Raydium AMM v4

Провести отдельный audit supported execution mode и effective reserves. Проверить status, OpenOrders/OpenBook/PnL dependencies и pool accounting по [официальному repository](https://github.com/raydium-io/raydium-amm). Для существующего `vault - needTakePnl` нельзя заявлять поддержку произвольного orderbook-integrated состояния.

Разрешается explicit restricted swap-only subset, если условия его достаточности доказаны и проверяются runtime. Остальные варианты получают `unsupported_pool_variant` с reason; молчаливого fallback в synthetic CPMM нет. Golden case должен показать, почему включённый вариант не требует отсутствующих accounts.

### 8.8. Exact-output и transfer fees

Обязателен native exact-output для synthetic CPMM и подтверждённого Raydium CPMM subset. Для остальных declared capabilities: native supported либо explicit unsupported. Bounded exact-in inversion допустим только offline, по одному immutable snapshot/route, с доказанной монотонностью и max iterations; при output≠target возвращать `exact_size_unavailable` для strict equality API.

`cost_to_acquire` означает net tokens получателя. При transfer fee нужно знать gross transfer, net pool input, output fee, epoch/max fee и возможные hooks. Первая поставка отклоняет непроверенные extensions. Никакого «почти тот же токен» и скрытого округления количества до perp lot.

## 9. Path executor и shared liquidity

Алгоритм pure `simulatePath(root, request)`:

1. Validate schema, root identity, scenario, caps, supported pools/tokens, execution domain и chain context.
2. Создать fresh overlay и scenario balance ledger.
3. Для каждой ноги разрешить amount_source; проверить asset adjacency/баланс и получить pool **из overlay**, если он уже использовался, иначе из root.
4. Запустить точный adapter; проверить consumed amounts, limits, statuses и token conservation.
5. Применить returned diff только к private overlay; обновить scenario balances. Записать before/after refs и movements.
6. При ошибке прекратить путь: сохранить prefix только как diagnostic trace, whole path complete=false. Исходное состояние неизменно.
7. При успехе вернуть state/movements по всем активам. Промежуточный dust не исчезает. Итоговый settlement PnL возможен только с корректной valuation/closure снаружи.

Repeated pool даже через другой provider alias видит прежние изменения. Два независимых candidate requests получают отдельные branches; нельзя суммировать их прибыль как одновременно доступную. Для совместного virtual allocation Portfolio Engine должен передать один упорядоченный план/включить предыдущие операции в branch. Глобальный allocator вне задания.

`require_complete` — default и единственный candidate mode v1. Partial trace может показывать consumed prefix, но не считается исполнением реальных legs и не получает eligibility. `all_or_nothing` здесь — политика возврата результата, не обещание атомарности сети.

## 10. Python/worker протокол

Старые configure/quote_request/shutdown сохраняют поведение. Добавить:

- `simulation_capabilities` в ready/descriptor: schema/model versions, supported variants, exact-in/out, post-state scope, consistency levels и caps.
- `snapshot_request` / `snapshot_result`: pool IDs, требуемая quality; capture только уже подготовленного state. На miss вернуть missing dependency descriptors без скрытого remote вызова.
- `simulate_path_request` / `simulate_path_result`: snapshot token, ordered plan, time budget; отдельные DTO и pending map.
- `export_simulation_evidence` / `simulation_evidence_result` либо эквивалентный bounded chunked export для сохранения выбранного root/projection.
- `cancel_simulation` по request ID; отмена best effort + обязательное игнорирование late response родителем.

Пример формы запроса (aliases A/B/P — только fixtures, в production canonical IDs):

```json
{
  "type": "simulate_path_request",
  "schema_version": 1,
  "request_id": "sim-42",
  "snapshot_ref": {"snapshot_id": "snapshot-7", "worker_generation": "worker-3"},
  "timeout_budget_ms": 250,
  "scenario_id": "frozen-immediate-v1",
  "execution_policy": "require_complete",
  "initial_balances": [{"asset_id": "A", "location": "simulation", "amount_raw": "100"}],
  "legs": [
    {"leg_id": "entry", "pool_id": "P", "mode": "exact_in", "input_asset_id": "A", "output_asset_id": "B", "amount_source": {"kind": "literal", "amount_raw": "100"}},
    {"leg_id": "exit", "pool_id": "P", "mode": "exact_in", "input_asset_id": "B", "output_asset_id": "A", "amount_source": {"kind": "previous_output", "previous_leg_id": "entry"}}
  ]
}
```

Пример сокращённый: обязательные policy/context/limits из DTO добавляются validator/default builder; defaults должны быть документированы и участвовать в hash. Не копировать сокращённый пример как полную production schema.

Snapshot token относится к worker generation. После restart старые futures завершаются `worker_restarted`, old tokens invalid. Не удерживать promises/overlays после timeout. Запрос с duplicate request_id в одном generation не должен перезаписать future.

Не запускать synchronous тяжёлый loop под видом asyncio concurrency: `Promise` не распараллеливает CPU. Разбивать длительный path на ограниченные вычислительные slices с yield между ними, сохраняя pinned immutable input, либо использовать bounded worker_threads после измерений. Внутри primitive допустим короткий sync loop; проверять cap/deadline на crossing boundaries.

Разделение purity и отмены: pure transition получает детерминированный work-unit cap и immutable context; dispatcher/cooperative runner между slices проверяет cancellation и local deadline. Не добавлять `Date.now()` в fee/price math. Исчерпание work units детерминированно; wall-clock timeout — orchestration outcome и не входит в economic hash успешного результата.

Evidence export: максимум 256 KiB payload на chunk, `export_id`, `chunk_index`, `chunk_count`, `total_bytes`, content hash. Проверять caps до выделения памяти, порядок/дубли chunks и итоговый hash; неполный export не регистрировать как готовое доказательство. Python stream reader limit и Node input limit конфигурируются согласованно, превышение лимита не приводит к бесконечному restart loop. Для обычного path result предпочтителен compact state diff; большие root bodies передаются только выбранному evidence export.

## 11. Broker, cache и acquisition budget

Расширить Broker отдельной типизированной операцией `simulate_local_path(AmmPathRequest, deadline_monotonic_ns) → AmmPathResult`. Существующий `estimate_local_path(QuoteRequest) → QuoteResult` сохранить как совместимый legacy API и явно отметить single-quote семантику. Не менять его return type по содержимому runtime request.

`local_path_backend` инъектируется в Broker при build; не использовать `_local_quote_source._client` из анализатора. Provider bridge управляет snapshot запросом через публичный source API; lifecycle source остаётся у supervisor.

Path cache key содержит: root dependency hash, ordered full legs, raw amounts, direction/mode, initial balances, chain context, model/SDK version, fee/slippage/route policy, execution policy и worker generation. Request ID, dependent consumer IDs и время публикации не входят в economic cache key. Сценарии разных balances/limits не получают ошибочный cache hit.

Кэшировать immutable result; clone envelope при доставке другому consumer, не переписывать economic state/timestamps. Freshness относится к dependencies, не compute completion. Negative gates зависят от snapshot/route/amount/capability; state update может разрешить новый расчёт, plain quote-result событие не должно запускать feedback loop.

Exact amounts cached remote quote можно использовать для old comparison, но нельзя им заменить sequential request, которому нужен state-after. У hidden aggregator path выставлять `liquidity_overlap_unknown`/unsupported; список pool IDs без account state и точного route execution порядка тоже недостаточен.

Предпочтительные API signatures (типы вводятся этим заданием, сейчас их нет):

```python
class LocalAmmBackend(Protocol):
    async def capture_snapshot(
        self, request: SnapshotRequest, *, deadline_monotonic_ns: int,
    ) -> SnapshotCaptureResult: ...

    async def simulate_path(
        self, request: AmmPathRequest, *, deadline_monotonic_ns: int,
    ) -> AmmPathResult: ...

    async def export_evidence(
        self, reference: SimulationEvidenceRef, *, deadline_monotonic_ns: int,
    ) -> SimulationEvidenceBundle: ...
```

TS adapter contract: `validateSnapshot(body, context)`, `simulateExactInput(body, leg, context, workBudget)`, optional `simulateExactOutput(...)`, `applyTransition(body, transition)`, `encodeState(body)`. `applyTransition` проверяет before-state hash и создаёт новое owned state; применение transition к другому root отвергается. Основной path executor вызывает эти операции вместе, не принимает произвольный fee/state diff от strategy consumer. Связанный расчёт capacity возвращает только известную область/coverage, не выдумывает полный maximum input при недостающих arrays.

Разделять local CPU budget и network acquisition quota. Чистая симуляция использует 0 remote requests. При cache miss и недостатке dependencies backend может **отдельно** попросить существующий acquisition layer о deduplicated refresh, затем захватить новый root и заново рассчитать целый path в пределах исходного deadline. Не смешивать первую ногу старого snapshot и вторую нового. Все refresh используют существующий shared Solana pacer и общий domain budget; не заводить новый Connection ради стратегии. Без подтверждённого refresh budget вернуть incomplete, а не активировать paid provider.

Одинаковый DEX path для нескольких perp venues вычисляется один раз; новый perp BBO вызывает только новый экономический расчёт, пока AMM dependency hash/TTL пригодны.

## 12. Подключение к анализаторам

### 12.1. Composition и feature flags

Добавить в config loader и `.toml.example` секцию `[amm_simulation]`: `enabled=false`, `mode="shadow"`, allowlist protocols/pool variants, consistency policy, limits из §14. При отсутствии секции поведение старого scanner сохраняется. Секреты/URLs сюда не добавлять.

В `unified_market_data.py` сохранить `_local_quote_source` под осмысленным именем и зарегистрировать один backend. Сконструировать shared service для consumers. Вычисление enqueue через coalesced dirty groups/shortlist, не await тяжёлой path simulation прямо на каждом market event handler. Result callback помечает зависимые routes dirty и имеет защиту от повторного запроса на том же key.

### 12.2. DEX/perp: обязательный end-to-end сценарий

Для allowlisted local pool и проверенного соответствия base/settlement:

1. Зафиксировать exact DEX размер; сверить perp quantity lattice. Если требуется другой размер, получить собственную native exact-out локальную покупку либо отказаться; не масштабировать прежний quote.
2. Одним path симулировать покупку base и немедленную продажу **полученного net base** на её post-state.
3. Подключить тот же current compatible perp BBO/depth, fees и freshness. При отсутствии достаточной глубины не повышать качество.
4. Посчитать immediate short-perp roundtrip как short price PnL минус entry/exit fees; notional short не является cash поступлением. DEX contribution — final stable minus initial stable. Gas reserve применяется снаружи один раз по плану операций.
5. Сохранить отдельный `analysis_kind="dex_perp_sequential_flat_model"`, `dex_post_trade_pool_state_simulated=true`, snapshot/evidence refs, scenario kind, AMM actual quantities, `execution_ready=false`.
6. Old `dex_perp_paired_exact_quote_*` записи остаются independent/pre-trade; неизвестные TON/EVM/RFQ/aggregator источники не повышаются.

По умолчанию shadow results не меняют live candidate lifecycle. Предусмотреть явный `mode="model_candidates"`: только complete/fresh/compatible evidence со всеми существующими quantity/fees/contract checks может участвовать в **публичных модельных** candidates. Сам boolean `post_trade=true` не снимает остальные blockers. Не включать этот режим в пользовательском local config автоматически.

Блокер `dex_paired_quotes_do_not_simulate_post_trade_pool_state` снимается только у конкретного нового sequential result. Он не снимает account fees, inventory/margin, wrapper/FX, chain consistency/inclusion или funding limitations. Sequential result не становится `paper_closed` без Position Engine. Future funding scenario остаётся явно projected и требует собственной временной модели, не реализуемой простым immediate path.

### 12.3. Spot/CEX и multi-pool

Сохранить single-swap results с текущей глубиной/комиссиями CEX; новый stateful backend сравнивать на одинаковом input/snapshot в shadow. `SolanaRouteEvaluator` при новом флаге идёт через Broker; не содержать второй math implementation.

Для S03/S04 модуль исполняет заданные legs, но не строит глобальный graph и не оптимизирует размеры. Обязательны offline и composition fixtures с двумя supported protocols в одном path и повторным pool. Для live multi-pool eligibility применяются stricter consistency gates, а не автоматическое разрешение по факту одной сети.

### 12.4. Exit на фиксированное количество

Python backend должен принимать immutable `position_quantity_raw=q0` и новое observed snapshot. Следующий quote на условные 100 stable с output q1 не заменяет q0. Здесь не реализуется полный storage позиции; достаточно typed input и integration test T12. Unknown borrow для short-spot по-прежнему блокирует стратегию.

## 13. Evidence, replay и диагностика

На диск не писать все market snapshots или каждую simulation. Хранить bounded counters/histograms и выбранные compact terminal/shadow discrepancy events. Для сохраняемого sequential candidate обязательна ссылка на replayable evidence: root projection нужных pools/accounts, ordered request, config/model/SDK versions, ожидаемые amounts и state hashes. Одного root hash без тела/доступной ссылки недостаточно для воспроизведения.

Предлагаемый каталог `market_data/amm_simulation/`: `stats.json`, `capabilities.json`, bounded `evidence/`, bounded discrepancy events. Отдельно указать retention budget, индексы и ref lifecycle. Не удалять evidence активного сохранённого candidate по LRU незаметно; при исчерпании бюджета прекратить новые evidence-dependent promotions с явным счётчиком. Общую ротацию исторического scanner journal не переписывать в рамках этого задания.

Stats: received/deduplicated/cache-hit/completed/failed/cancelled; reason counts; active queue/tasks/snapshots/items/bytes; pool/variant capabilities; dependency age и quality counts; compute/capture/IPC/queue end-to-end p50/p95/p99; ticks/bins/legs; capture refresh requests и quota denials; late responses; eviction; state mismatch at publication; evidence retention exhaustion.

Capability record: protocol/variant, model version, package version, program/source revision, positive/negative fixture IDs, verified date, exact-in/out, post-state scope, supported consistency и unsupported extensions. `supported` требует успешных tests; `unknown` отличается от `unsupported`.

Replay command должен работать offline из одного evidence bundle и сравнивать amounts/state hashes, исключая wall-clock timing поля. Ошибка или отсутствующее доказательство — exit code nonzero. Evidence не содержит endpoints, query strings, API keys, wallet/account credentials. Snapshot bytes только публичных protocol accounts; path refs не должны позволять произвольное чтение файлов.

## 14. Ограничения ресурсов и эксплуатация

Ниже предлагаемые стартовые локальные caps, не измеренные характеристики и не квоты RPC:

| Настройка | Default | Требование |
| --- | --- | --- |
| `max_path_legs` | 4 | Жёстко проверить до работы |
| `max_pending_paths` | 64 | Coalescing identical keys; excess typed refusal |
| `max_inflight_paths` | 2 | Не складывать с unlimited legacy work |
| `max_snapshots` | 64 | Общий byte cap важнее item cap |
| `max_snapshot_bytes` | 64 MiB total | Включить overlays/results; описать метод оценки |
| `max_result_cache_items` | 512 | Вместе с byte cap, dependency invalidation |
| `max_crossings_per_path` | 1024 | Единый предел ticks/bins; protocol caps могут быть меньше |
| `max_search_iterations` | 64 | Только offline exact-in inversion |
| `path_deadline_ms` | 250 | Измерять wall time, включая queue/IPC; shorten по caller |
| `max_json_line_bytes` | 1 MiB | Совместимо в Python/Node; evidence chunks bounded |
| `evidence_budget_bytes` | 128 MiB | Не raw streaming; явная политика при exhaustion |
| `stats_flush_seconds` | 10 | Периодическая компактная запись |

TTL не делать универсальными «250 ms». Reuse существующие policy класса данных и требовать per-dependency integrity/validated age. Freshness нельзя ослабить для прохождения performance benchmark. Любая смена defaults — в report с измерением и обоснованием.

Offline benchmark: минимум 10 000 warm CPMM paths и 1000 каждого tick/bin fixture класса, 1/2/4 ноги, median и p95/p99, cold capture отдельно. Проверить нагрузку вместе с синтетическим event stream и cancellation. Цели на baseline машине: p95 pure CPMM ≤5 ms; p95 bounded CLMM/DLMM ≤50 ms на явно указанном числе crossings; p99 event-loop stall ≤20 ms при slicing; memory в caps. Это инженерные acceptance targets; при недостижении показать reproducible profile и ограниченный охват, не скрывать failure средним значением.

30-минутный offline overload/soak обязателен для полного модуля: memory plateau, отсутствие pending futures после отмен, нет starvation control/shutdown. Live smoke — отдельная опциональная read-only проверка уже согласованного режима; отсутствие текущего live сервиса не препятствует offline приёмке. Общий 24h soak M6 не считается выполненным данным тестом.

## 15. Тесты и критерии приёмки

Каждый AS-ID — observable contract; один ID может иметь несколько parametrized/subTests. Pure tests не требуют сети/ключей/активного сканера. Expected amounts для critical fixtures получены вручную/независимым reference/закреплённым contract test, не сохранены из output тестируемой функции.

| ID | Проверка | Обязательный outcome |
| --- | --- | --- |
| AS01 / T09 | Synthetic 1000/1000, fee=0, 100→90→99 | Точные amounts, states 1100/910 и 1001/1000 |
| AS02 | Независимый reverse quote на S0 | 82; не подставляется вместо 99 |
| AS03 | Synthetic fee 1% | 100→90→97, fees 1/1, state 1003/1000, conservation |
| AS04 | Exact-out target90 на S0 | Input99 при fee0; 98 недостаточно |
| AS05 | Raw sizes: 0, negative, bool, exponent, >2^53, u64/u128 boundaries | Invalid отклонены; большие валидные точны; contract overflow отвергнут |
| AS06 | Один snapshot, два concurrent paths разных размеров | Оба совпадают с независимыми roots; live objects/hash unchanged |
| AS07 / T17 | Повторный pool через alias в path | Второе обращение видит изменённое состояние |
| AS08 | Multi-pool A→B→C→A и повтор pool | Net output передаётся между legs, fee ровно раз, balances сходятся |
| AS09 | Нет средств на leg2 или asset/location mismatch | Complete=false; root не изменился; prefix не итоговый fill |
| AS10 | Missing array после одного crossing | insufficient_state_coverage; никакой extrapolation |
| AS11 | Доказанное отсутствие liquidity vs missing data | Разные статусы, разные retry policy |
| AS12 | CLMM exact boundary + negative ticks, оба направления | Contract rounding/current tick/liquidityNet согласованы |
| AS13 | CLMM несколько arrays, reverse после crossing | Проверенный second-leg result и sufficient after-state |
| AS14 | DLMM несколько bins и reverse | Bin balances/active ID/variable fee state переносятся |
| AS15 | CPMM все feeOn settings × оба направления | Правильный fee asset, vaults/counters, net output |
| AS16 | Trade fee разделяется на LP/protocol/fund | Не задваивается, резерв второго swap корректен |
| AS17 | Unknown token hook/transfer extension/adaptive variant | Explicit unsupported, не нулевая fee |
| AS18 | AMM v4 supported subset и OpenBook-dependent variant | Verified fixture либо targeted отказ, не fallback |
| AS19 / T22 | Два одинаково старых snapshot/quote | Freshness fail несмотря на малый skew |
| AS20 / T23/26 | Новый source epoch/worker generation, поздний response | Старый не публикуется live; futures закрыты |
| AS21 | Dependency update во время await capture/compute | Не mixed snapshot; revalidation перед promotion |
| AS22 | Изменение fee config/tick без изменения pool account | Hash/versions/cache invalidation изменяются |
| AS23 | Same slot, разные read batches/WS updates | Не повышается до pinned consistent |
| AS24 / T24/25/27 | 10 consumers одного key, новый perp tick | Одна local compute, 0 remote при ready snapshot; повтор cache |
| AS25 / T28/29/34 | Missing state + cooldown/rate-limit | Dedup refresh и общий quota; нет feedback loop |
| AS26 / T30/35 | Queue/bytes/crossing caps, hanging worker | Bounded; typed отказ; контроль/shutdown обслуживаются |
| AS27 / T32 | UTC jump/suspend/reboot | TTL не продлевается, old boot invalid |
| AS28 | Cancel одного из 10 dedup waiters | Остальные получают результат; shared task не убит преждевременно |
| AS29 | Все waiters отменены/timeout, поздний worker result | Нет leaked future/overlay, нет live promotion |
| AS30 / T11 | DEX quantity не кратно perp lattice | Новый exact quote или explicit отказ; scaling нет |
| AS31 / T12 | Position q0, новая notional quote даёт q1 | Exit input=q0 на свежем observed state; вход не применяется повторно |
| AS32 | Aggregator pair/unknown overlap | Старый paired model остаётся unqualified |
| AS33 | Typed sequential result spoof: boolean без hashes/steps | Не получает sequential quality/eligibility |
| AS34 | Immediate DEX + short perp, funding=0, nonnegative fees | Нет прибыли от notional short или одного basis |
| AS35 | Gas/network reserve + fees mapping | Не задвоены pool fees/gas, raw movements сохранены |
| AS36 | Shadow flag off/on | Off сохраняет legacy contracts; on не создаёт новых collectors |
| AS37 | Evidence canonical hash/replay Python↔TS | Economic hash идентичен, timings не мешают replay |
| AS38 | Corrupt/missing/oversize JSONL, unknown schema | Controlled failure, secrets не попадают в stdout/stderr |
| AS39 | Unknown endpoint credentials в error | Redacted path/query/userinfo; bounded diagnostic |
| AS40 | Slippage/price limit на последнем шаге | Whole path incomplete; diagnostic prefix не кандидат |
| AS41 | Evidence retention cap | Явный отказ сохранения/повышения, нет broken live reference |
| AS42 | Новый state update отменяет прежний отрицательный результат | Новый key может вычислиться; plain retry без изменений ограничен |

Property tests с fixed seed: token conservation по каждому asset; determinism; isolation; no positive self-roundtrip для поддержанного synthetic unchanged-market случая с неотрицательной fee; no negative reserves; full fill consumed amounts корректны; increasing fee не улучшает output в фиксированной synthetic модели; sequential amounts соответствуют объявленным limits. Не переносить свойства monotonicity/concavity на произвольный aggregator или dynamic-fee variant без доказательства.

Для каждого включённого реального adapter минимум 3 размера × 2 направления, 2 sequential paths, boundary и error cases. Для concentrated/bin models нужны crossing и exhausted coverage. Differential comparison на **одинаковом** snapshot/chain context; state mismatch — отдельная категория. Reference не должен вызывать тот же production transition под другим именем.

## 16. Стайлгайд и качество реализации

### 16.1. Общие соглашения

- Следовать локальному стилю изменяемого файла; не выполнять массовое форматирование соседних модулей. Имена отражают units: `_raw`, `_ns`, `_ms`, `_bps`, `_x64`, `_quantity`, `_asset_id`.
- Новые поля денег не называть `_usdt`, если фактическая валюта может быть USDC. Legacy aliases сохранять только в serializer, рядом выводить `pnl_currency`.
- Код/docstrings/comments — английский, operator docs — русский. Комментарии объясняют rounding, protocol edge cases, ownership и причины, а не повторяют строку кода.
- Функция выполняет одну ответственность. Математика, validation, transport, scheduling и reporting разделены. Нет универсального `utils.py` со всем подряд и framework ради пары adapters.
- Нет скрытых global mutable caches, monkey patches production SDK, `eval`, динамического импортирования неподтверждённого adapter из входного JSON.
- Обязательные публичные DTO валидировать на boundary; внутренние функции работают с typed validated state. Не распространять `dict[str, Any]`/`any` по pure core.
- `None`/`null` означает неизвестно, а не 0. `unsupported` не заменяется дефолтной liquidity/fee. Fallback стратегии явно именованы и не повышают quality.

### 16.2. Python

- `from __future__ import annotations`; 4 пробела, double quotes, snake_case functions/modules, PascalCase classes, UPPER_SNAKE_CASE constants.
- Импорты stdlib → external → project, абсолютные project imports; `collections.abc` для Mapping/Sequence/Callable. Не импортировать heavy transport/SDK при импорте pure DTO.
- Keyword-only параметры для неочевидных однотипных аргументов, especially quantities/timestamps/IDs. Полные type annotations на public API.
- Frozen dataclasses + slots для immutable DTO; вложенные MappingProxyType/tuples или валидированные immutable objects. Документировать ownership, не считать замороженность оболочки достаточной.
- `int` для raw, Decimal из strings/int; reject bool отдельно. Не создавать Decimal из float, не менять global Decimal context. Если нужен display precision, `localcontext` с явной границей.
- Узкие except clauses. `asyncio.CancelledError` не поглощать. Cleanup через finally; pending futures завершаются один раз. Ожидаемый domain failure — typed status, programmer invariant violation — exception с boundary conversion.
- Стремиться к 100 символам строки в новых модулях, оставлять existing formatter conventions. Не вводить обязательный Ruff/Black/mypy rollout на весь проект ради этого модуля.

### 16.3. TypeScript

- 2 пробела, double quotes, semicolons, trailing commas для multiline. PascalCase types/classes, camelCase runtime functions/fields; wire JSON — snake_case согласно Python DTO.
- Сохранить `strict`, ES2022, NodeNext; относительные imports с `.js`. `import type` для type-only imports.
- Boundary input — `unknown`, затем type guards. Exhaustive `switch` на discriminated union с `never` guard. Не использовать `as any`, `@ts-ignore`, non-null assertion для подавления отсутствующей liquidity/account.
- BN cloning явный, не вызывать mutating `iadd/isub` на shared input. BigInt/BN conversion только через строки/точные primitives; `.toNumber()` запрещён для amounts/liquidity/price.
- `ReadonlyMap` предотвращает запись через тип, но не заменяет private ownership. Live SDK instance с network methods не передаётся в pure adapter.
- stdout только JSONL protocol; диагностические stderr records bounded/redacted. Никакого `console.log` в расчётной библиотеке.
- Извлечённую upstream арифметику сопровождать license/attribution и commit/version. Не править `node_modules`; не менять lockfile без необходимости и описания diff.

### 16.4. Тесты и документация

- Python: `unittest.TestCase`/`IsolatedAsyncioTestCase`, fake clock/worker/budget, tempfile для evidence. TS: `node:test`, `node:assert/strict` как в существующем worker.
- Не использовать реальные sleep/сеть для проверки математических/TTL условий. Async teardown проверяет, что task/future не осталось.
- Test names описывают поведение. Expected numbers явно записаны в fixture + derivation/provenance. Fixtures малы, raw amounts strings, versioned, без secrets и случайных live timestamps.
- Тестировать поведение публичного контракта, не порядок приватных вспомогательных вызовов. Integration assertions на число collectors/requests нужны для архитектурной гарантии dedup.
- Не добавлять новые зависимости, если достаточно текущего toolchain. Property loop с fixed seed допустим без установки библиотеки.

## 17. План работы агента и точки сдачи

1. **Baseline и source audit.** Прочитать общий план и карту §3, выполнить tests, зафиксировать версии. Для каждого adapter составить dependency/rounding/fee transition matrix и источник истины. Выяснить применимость program revision, не предполагать её по ticker.
2. **Контракты и synthetic oracle.** Добавить DTO/codec/status, pure CPMM и tests AS01–09/37; показать численный T09. До следующего шага добиться детерминированности и deep isolation.
3. **Snapshot registry и transport.** Immutable capture, generation, coverage, pending futures, bounded messages/cancel. Старый quote protocol остаётся совместимым. AS19–29/38.
4. **Raydium CPMM.** Правильный fee direction/counter accounting, exact-in/out и reverse fixtures. Не отмечать supported до after-state проверки.
5. **Orca и первый end-to-end срез B.** Tick crossing, Broker integration, local DEX/perp shadow fixture, zero additional collector guarantee. Сдать intermediate report со списком ещё незавершённых C работ.
6. **Raydium CLMM, Meteora, AMM v4 subset.** Довести matrices/fixtures и mixed-protocol paths; unknown variants явно закрыты.
7. **Evidence/replay, performance, soak.** Полная таблица AS-ID → tests → outcome, measured caps/latency/memory, report о корректности и различиях старого/нового расчёта.
8. **Финальная передача.** README usage, example config disabled, capability matrix, operator limitations, контрольный replay bundle, полный regression log. Не включать feature в live config и не инициировать live rollout самостоятельно.

На каждом шаге код должен оставаться запускаемым с disabled feature. Можно сделать небольшое необходимое исправление существующего validator/fee mapping, если обнаружен конфликт контракта; добавить целевой тест, описать scope, не переписывать unrelated engines.

## 18. Проверка и Definition of Done

Команды baseline из корня проекта:

```bash
.venv/bin/python -m unittest discover -s tests
```

Из `workers/solana-quote-worker`:

```bash
npm run check
npm test
```

Добавить документированные offline commands для fixture replay/benchmark/soak. Для нового node test runner явно проверить discovery всех новых test files: summary «1 test file» существующего runner не означает, что все будущие fixture cases обнаружены. Tests не должны требовать установку новых пакетов или обращения к live API при каждом запуске.

Полная приёмка:

- Все обязательные stages A–C реализованы; capabilities по protocol variants соответствуют тестам. Generic CPMM-only работа не принимается как весь модуль.
- T09 однозначно исправлен в новом документированном test case, никакой фабрикации возврата 100 при raw integer units.
- Повторные swaps используют post-state, а observed state остаётся неизменным даже при concurrent calls/cancel/errors.
- Все economic amounts integer-exact и token conservation проверено; fee counters/gross/net/raw identity понятны из evidence.
- Complete local snapshot даёт 0 remote calls для path; dedup across consumers; limits/deadlines/invalidation работают.
- Новый DEX/perp sequential-result проходит end-to-end integration tests, old paired rows не получают ложное повышение, live rollout disabled.
- Все сохранённые promoted sequential results имеют replayable evidence либо явный отказ в promotion при его отсутствии.
- Regression Python/TS пройдена; performance/soak отчёт воспроизводим и фиксирует неуспешные targets, если они есть.
- Переданы изменения кода, tests/fixtures, capabilities, README/config example, migration note, source/version/license notes, known limitations, acceptance matrix.
- Финальное сообщение исполнителя содержит реализованные варианты, тестовые результаты, фактические ограничения и точную команду offline replay. Не писать «всё готово», если остались обязательные adapters/after-state verification.

## 19. Источники для исполнителя

Первичны локальные интерфейсы и pinned lockfile; ссылки ниже — места для сверки протокола, не разрешение автоматически перейти на latest upstream. Зафиксировать конкретный commit/version использованного алгоритма в capability/fixture metadata.

- [Raydium CPMM program](https://github.com/raydium-io/raydium-cp-swap), особенно curve/calculator, states/pool и swap handlers.
- [Raydium CPMM fees](https://docs.raydium.io/products/cpmm/fees).
- [Raydium CLMM program](https://github.com/raydium-io/raydium-clmm).
- [Raydium AMM v4 program](https://github.com/raydium-io/raydium-amm).
- [Orca Whirlpool program и SDK](https://github.com/orca-so/whirlpools), swap manager/math/ticks и соответствующая legacy SDK version.
- [Meteora DLMM SDK](https://github.com/MeteoraAg/dlmm-sdk) и [официальная SDK reference](https://github.com/MeteoraAg/docs/blob/main/developer-guides/dlmm/typescript-sdk/reference.mdx).
- [Solana getMultipleAccounts RPC](https://solana.com/docs/rpc/http/getmultipleaccounts).

## 20. Короткое задание для запуска другого агента

> Реализуй `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md` в текущем Crypto market-data lab. Сначала сверь общий план `TECH_SPEC_STRATEGY_ENGINE.md`, существующие interfaces и baseline tests. Выполняй этапы A–C из ТЗ: immutable snapshots, stateful exact AMM path simulation, protocol adapters, Broker/worker integration, shadow DEX/perp consumer, evidence/replay и acceptance tests. Сохраняй legacy API и чужие изменения. Не меняй local secret config, не запускай/не перезапускай live scanner и не добавляй торговые операции. Не выдавай независимые pre-trade quotes за post-state. Соблюдай стайлгайд §16 и отчитайся по Definition of Done §18. Если protocol variant не подтверждён, явно закрой capability и укажи, что именно ещё мешает полной приёмке.
