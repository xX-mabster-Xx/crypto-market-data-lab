# Аудит готовности market-data-lab — 12 сентября 2026

## 1. Краткий вывод

Проект уже полезен как **read-only исследовательский сканер публичного рынка**: сбор котировок, проверка свежести, поиск модельных расхождений, оценка некоторых циклов и funding-сценариев. Это не завершённый Strategy Engine из общего ТЗ и не торговая система.

Новый post-trade AMM-код существенно расширил offline-ядро, но **сквозная AMM-функциональность не готова к приёмке**. Есть рабочие синтетические расчёты и SDK-fixtures для CPMM/Orca, однако основной сканер не подключает новый backend. Найдены конкретные ошибки интерфейсов, snapshot freshness и протокольной математики. Включение одного флага этого не исправит.

Статусы ниже означают:

- **Работает в текущем контуре** — вызывается основным сканером, есть тесты и/или свидетельства предыдущего запуска; доступность внешних API именно сегодня отдельно не подтверждалась.
- **Работает offline / частично** — отдельный модуль исполняется, но это не доказательство готовности всего пользовательского сценария.
- **Не готово** — отсутствует нужный сквозной сценарий либо найден блокирующий дефект.

Процент готовности не назначаю: он скрыл бы разницу между наличием файлов, правильной математикой и работающей интеграцией.

## 2. Что и как проверено

Основа оценки: `TECH_SPEC_STRATEGY_ENGINE.md`, особенно разделы 18–20 и этапы M0–M6; `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`; фактический composition root `scanner.py → unified_market_data.py`; источники, анализаторы, Broker, cost/funding/state-модули; Python- и TypeScript-реализации AMM; тесты, benchmark и сохранённые результаты запуска.

Заявления `README_AMM_SIMULATION.md` сопоставлены с кодом, а не приняты за подтверждение готовности. Аудит не изменял исполняемый код и конфигурацию, не запускал live-сбор, не отправлял заявки/транзакции и не перезапускал процессы. Создан только этот отчёт. Проверка не является независимым аудитом всех внешних протоколов и всех адаптеров бирж.

Повторно выполнено в текущем окружении:

| Проверка | Фактический результат | Что подтверждает |
| --- | --- | --- |
| `.venv/bin/python -m unittest discover -s tests` | 251 тест, OK, около 1,4 с | Регрессия имеющихся Python-тестов |
| TypeScript `npm run check` | Успешно | Типизация worker-проекта |
| TypeScript `npm test` | 35 тестов, 35 pass | Имеющиеся TS-fixtures и unit-тесты |
| `npm run benchmark` | Успешно; CPMM p95 для 1/2/4 ног: 0,004/0,008/0,009 мс; heap 14 MiB | Узкий локальный CPMM microbenchmark |
| Дополнительная runtime-проба Broker | Действующая сигнатура не та, что у typed AMM API | Подтверждает дефект F02 |
| Дополнительная runtime-проба worker path | Без балансов `insufficient_balance`, с балансом `complete` | Подтверждает дефект F03 |
| Дополнительная runtime-проба TTL | Просроченный snapshot принят с `complete=True` | Подтверждает дефект F06 |

**286 проходящих тестов не означают 286 подтверждённых требований.** В частности, проверки CLMM/DLMM на положительный output и согласованность собственных fixtures не проверяют совпадение с независимым эталоном реального протокола. Проверка типов также не обнаружила runtime-проблему отсутствующих балансов.

## 3. Готовность по общему плану M0–M6

| Этап | Что уже есть | Что мешает закрыть этап | Вердикт |
| --- | --- | --- | --- |
| M0. Семантика и baseline | Модели идентичности/контрактов/комиссий; разделение basis и PnL; fixtures; явные quality/blocker-поля | Нет полного подтверждённого coverage/units-аудита всех подключённых рынков; исторический экстремальный BONK-сигнал требует расследования | Существенно выполнен, не принят полностью |
| M1. Версионированное состояние | Event envelope, boot/source epoch, отсев старых/повторных событий, bounded state, общий bus для анализаторов | Новый AMM snapshot-контур не соблюдает тот же уровень dependency/version/consistency-контрактов; полная отказная приёмка не доказана | Рабочая базовая инфраструктура, AMM-расширение незавершено |
| M2. Cost Engine и Broker | L2-оценка, fee currencies, lattice sizing, exact-quote pair cache, Broker cache/dedup/budgets/cooldown/deadlines, локальный пересчёт | Post-state AMM не готов end-to-end; конфликт API; нет полной FX-модели и остаточного position-ledger; динамические backends основного Broker пусты | Частично готов; главный текущий приоритет |
| M3. Spot-search | CEX↔CEX, CEX↔DEX и ограниченные CEX→DEX→CEX треугольники | Нет общего поиска S01–S05, multi-hop post-state, полного size optimizer и exhaustive-oracle приёмки; нет ресурсного плана ребаланса | Подмножество работает |
| M4. Derivatives/carry | Linear-perp модели, BBO-size checks, entry/exit сценарии, funding-проекция по календарю при наличии метаданных | Нет полноценной позиции, borrow lifecycle, residual/unwind, S09/S10; нет требуемых двух DEX execution моделей end-to-end | Исследовательские модели работают, этап не завершён |
| M5. Portfolio/evidence | Candidate lifecycle, ограниченные журналы, quality-поля; offline AMM evidence/hash/replay | Нет общего portfolio reservation, ресурсных конфликтов, partial fill ledger, recovery позиций, decision replay каждого live-кандидата | Начальная/частичная реализация |
| M6. Производительность/приёмка | Unit-регрессия, локальный benchmark, старый длительный live-запуск | Нет подтверждённого W1 или согласованного меньшего профиля; нет принятого 24-часового soak текущей версии, полного fault injection/coverage/replay | Не выполнен |

Первый полезный инкремент, указанный в §19.4 общего ТЗ — M0–M2 плюс одна полная DEX↔perp цепочка — пока тоже нельзя считать закрытым. Само появление пары точных DEX-котировок и нового AMM-пакета эту цепочку не завершает.

## 4. Что уже можно запускать и видеть

### 4.1. Основной сканер

Обычная точка входа из корня проекта:

```bash
.venv/bin/python scanner.py
```

Ограниченный запуск для проверки доступности источников:

```bash
.venv/bin/python scanner.py --duration-seconds 300
```

Это команды для следующего запуска, **в ходе аудита они не выполнялись**. Имеется локальный RPC-конфиг. Новая проверка доступности RPC/API нужна перед выводами о сегодняшнем покрытии. Включать `[amm_simulation] enabled=true` для получения «готовой» AMM-аналитики сейчас не следует: backend не подключён, а указанные ниже дефекты не исправлены.

Сканер собирает публичные CEX spot-книги, perp-котировки/контрактный контекст, состояния настроенных Solana-пулов и polling exact-input DEX quotes. Наличие источника в конфигурации не гарантирует, что он в данный момент даёт пригодные данные. Локальные pool feeds и polling exact quotes — разные пути; не каждый наблюдаемый пул автоматически участвует во всех стратегиях.

### 4.2. Стратегии

| Стратегия | Что сейчас реально увидим | Ограничение |
| --- | --- | --- |
| S01: CEX spot↔CEX spot | `spot_spot_inventory_cycle`: расхождения между площадками после модельных комиссий | В этом анализаторе BBO, а не полный L2; предполагается заранее размещённый инвентарь |
| S02: CEX spot↔DEX spot | `direct_inventory`: оба направления, exact DEX quote, проход CEX-глубины, fees и minimum network reserve | Инвентарь, переводы, реальный gas/включение в блок и account fees не подтверждены |
| S04, ограниченный вариант | `cex_dex_cex_triangle`: заданные CEX→DEX→CEX маршруты | Не общий поиск произвольных 3–4-leg путей; нет глобальной оптимизации размера |
| S05: cross-venue/cross-chain inventory | Отдельные inventory-модели для доступных маршрутов | Нет полноценного восстановления остатков, bridge/transfer cost и resource planning |
| S06: long spot + short perp | `spot_perp_flat_price_cycle`, `spot_perp_funding_carry` | Frozen/flat-price exit-сценарии, BBO, гипотетический капитал; не фактическая доходность удержания |
| S07: short spot + long perp | Обратное направление можно рассчитать как сценарий/операцию с имеющимся spot | Не доказана исполнимость заёмного short: borrow, лимит, ставка, отзыв и возврат не реализованы полностью |
| S08: long perp A + short perp B | `perp_perp_flat_price_cycle`, `perp_perp_funding_carry` | Только совместимые контракты и settlement; отдельные margin/liquidation/portfolio ограничения не моделируются полностью |
| DEX spot + perp, вход | `dex_perp_entry_hedge`: точный размер входа и разница basis | Это не закрытый PnL; incompatible lot size отклоняется, а не масштабируется |
| DEX spot + perp, независимая пара quotes | `dex_perp_paired_exact_quote_flat_model` и `...funding_scenario` при совпадении raw base quantity | Две pre-trade quotes не образуют post-trade round trip; `candidate_eligible=false` |
| S03: DEX↔DEX / повторное использование пула | Offline пути на fixtures | В основном live-сканере полный post-state поиск не готов |
| Новый sequential DEX/perp | Есть injectable `dex_perp_sequential_flat_model` | Из обычного `scanner.py` не подключён; текущая реализация имеет блокирующие дефекты |
| S09: funding-event capture | Есть строительный блок календарной funding-проекции | Нет законченной стратегии с eligibility/latency/position lifecycle |
| S10: перенос существующего хеджа | Нет законченного пользовательского сценария | Нужны существующая позиция и сравнение switch против hold с полной стоимостью |
| S11–S13 | Не заявляются готовыми | Расширения после базовой приёмки: dated futures, maker+hedge, LP accounting |

Funding-модуль уже не просто умножает произвольную текущую ставку на часы: он различает отображаемую нормализацию и события выбранного горизонта, требует нужных метаданных. Но будущая ставка остаётся **проекцией**, а не обещанным начислением. Модель полноценного удержания позиции ещё не реализована.

Все эти результаты — research. `candidate_eligible=true` означает прохождение локальных модельных фильтров, **не разрешение торговать**. `execution_ready` остаётся false. Положительный edge без инвентаря, account fees, borrow/margin и модели исполнения не равен доступной прибыли.

### 4.3. Где смотреть результаты

Для `RUN = data/live/scanner/<run_id>`:

| Файл | Содержимое |
| --- | --- |
| `data/live/scanner/latest.json` | Указатель на последний запуск; обязательно проверять время обновления |
| `RUN/manifest.json`, `RUN/status.json` | Параметры и состояние общего процесса |
| `RUN/market_data/status.json` | Источники, свежесть/ошибки/restarts, состояния анализаторов и Broker |
| `RUN/market_data/cycle_analysis/stats.json` | Счётчики, лучшие direct/triangle маршруты и активные модельные сигналы |
| `RUN/market_data/cycle_analysis/candidate_events.jsonl` | Ограниченный журнал candidate lifecycle |
| `RUN/market_data/perp_analysis/stats.json` | Лучшие модели, не прошедшие квалификацию модели, entry-разведка, причины отказов |
| `RUN/market_data/perp_analysis/capabilities.json` | Зарегистрированные возможности perp-анализа |
| `RUN/market_data/perp_analysis/candidate_events.jsonl` | Ограниченные terminal summaries; не запись каждого тика и не торговый журнал |

В консоли появляются starts/improvements подходящих сигналов. Raw тики/книги целиком не сохраняются; поэтому старый сигнал нельзя автоматически превратить в полноценный replay принятого решения. Смотреть следует не только максимальный bps, но и доступный размер, денежный эффект, возраст, persistence, качество комиссии и blockers.

## 5. Готовность нового AMM-модуля по его отдельному ТЗ

| Часть | Подтверждено | Не подтверждено / сломано |
| --- | --- | --- |
| Stage A: синтетическое ядро | Immutable contracts, raw integer exact-in/out, локальные post-state overlays, повтор пула, balance propagation, evidence/replay; round trip 100→90→99 и fee-case 100→90→97 | Live TTL/worker/API требования не закрываются этими тестами |
| Raydium CPMM | Python/TS fixtures против закреплённого SDK, exact-in/out, fee split, vault/counter post-state; worker capture seam | Сквозной вызов через scanner/Broker/worker не работает; consistency/lifecycle snapshots не приняты |
| Orca classic Whirlpool | Offline exact-in/out, price/tick math, post-state path/replay, fixtures против SDK | Worker capture dispatch в live-пути остаётся CPMM-only; полного Stage B integration slice нет |
| Raydium CLMM | Контракты, functions, synthetic unit-тесты | Упрощённая математика вместо принятого protocol-exact расчёта; см. F04 |
| Meteora DLMM | Контракты, functions, synthetic unit-тесты | CPMM-подобный расчёт внутри одного bin вместо требуемой модели; см. F05 |
| Raydium AMM v4 subset | Есть restricted swap-only adapter и негативные проверки некоторых неподдерживаемых вариантов | Нельзя считать законченной протокольной приёмкой без независимого эталона subset accounting и live capture; не разрешение на все AMM v4 пулы |
| Broker/config/analyzer | Feature flag, bridge methods, injectable sequential result, отдельный analysis kind | Отсутствует production wiring; duplicate API; freshness/quantity binding; см. F01–F07 |
| Evidence | Canonical hashes и offline replay существующих bundles | Нет полной live-цепочки candidate → неизменяемый request/snapshot/result/evidence → replay |
| Performance | Быстрый warm CPMM на малом fixture | CLMM/DLMM, churn/TTL caps, actual event-loop delay, долгий soak и профиль основного сканера не измерены этим benchmark |

Итого: **Stage A имеет рабочую основу; Stage B выполнен частично; Stage C не принят.** Формулировки README «All stages A–C implemented — Done», «Capabilities match tests — Done» и заявление о регистрации backend в composition root не отражают фактическую готовность.

## 6. Дефекты, блокирующие AMM-приёмку

Приоритет P0 здесь означает «исправить до использования нового AMM для выводов о PnL», а не утверждение о текущем торговом инциденте: торгового контура нет, AMM в основной запуск не подключён.

### F01 — P0: отсутствует сквозное подключение

В `src/market_data_lab/unified_market_data.py:232` локальный source возвращается как `_local_quote_source`, но не передаётся новому sequential simulator. `UnifiedPerpAnalyzer` создаётся без `sequential_amm_simulator`; `QuoteBroker` создаётся с `backends={}`. Добавленные config/source seams сами по себе этого не меняют.

Последствие: обычный scanner не выдаст новые sequential AMM-строки после простого включения флага. README описывает отсутствующую интеграцию.

Приёмка исправления: тест именно реального composition root с включённым флагом и fake transport, затем read-only smoke на разрешённых пулах; наблюдаемый путь snapshot → simulate → отдельный analyzer result → evidence. С выключенным флагом прежняя аналитика не меняется. Не подключать до исправления остальных P0.

### F02 — P0: второй метод Broker затеняет typed AMM API

В `src/market_data_lab/quote_broker.py:809` есть `simulate_local_path` для typed AMM request с отдельным deadline. В том же классе на строке 943 снова определён `simulate_local_path(self, request: QuoteRequest)`. Python использует последний метод.

Runtime-проверка `inspect.signature(QuoteBroker.simulate_local_path)` возвращает:

```text
(self, request: 'QuoteRequest') -> 'QuoteResult'
```

Вызов предполагаемого API с `deadline_monotonic_ns=` получает `TypeError`. Прохождение теста dedup для второго API не проверяет первый API. Кроме того, первая версия метода сама по себе не использует переданный отдельный deadline по назначению.

Нужно выбрать один контракт нового AMM API, сохранить отдельно legacy `estimate_local_path` и покрыть публичную сигнатуру интеграционным тестом.

### F03 — P0: worker не передаёт начальные балансы

`workers/solana-quote-worker/src/worker.ts:266` вызывает `simulatePathLegs(bundle, legs)` без третьего аргумента. В `simulation/path.ts` начальные балансы по умолчанию пусты; первая положительная exact-in нога проверяет доступный баланс и отказывается. `SimulatePathRequestMessage` в `protocol.ts:50` также не содержит initial balances.

На том же CPMM fixture из `test/path.test.ts`:

```text
balancesProvided=false → status=insufficient_balance, complete=false
balancesProvided=true  → status=complete, complete=true
```

Нужны согласованные Python/JSONL/TS request contracts, валидация initial balances и тест реального request-handler пути. Нельзя устранять проверку баланса: нужно передать корректный ресурсный контекст.

### F04 — P0: Raydium CLMM не является точной протокольной моделью

`src/market_data_lab/amm_simulation/adapters.py:632`, `_clmm_compute_swap`, прямо описан как simplified. Использует упрощённый сдвиг sqrt price, не проходит реальные tick arrays, сохраняет tick/liquidity и сообщает `ticks_crossed=1`.

В TS `simulation/raydiumClmm.ts:125` tick→sqrt-price также заменён явно помеченной линейной аппроксимацией. Python и TS реализуют разные упрощения. Наличие типов tick arrays и положительного output не подтверждает требуемые tick crossing, liquidity transition, rounding и exact-out минимальность.

Нужно убрать статус protocol-exact/supported для неподтверждённой модели либо реализовать точный выбранный протокольный subset. Приёмка: независимые SDK/program fixtures в обе стороны, crossing нескольких ticks/arrays, нехватка ликвидности, price limit, fee/state transitions и минимальный exact-out input. Согласованность двух собственных реализаций недостаточна без внешнего эталона.

### F05 — P0: Meteora DLMM подменён однобиновой CPMM-подобной моделью

`adapters.py`, `_dlmm_compute_swap` около строки 690, и TS `simulation/meteoraDlmm.ts`, `dlmmComputeSwap`: output рассчитывается через `reserve_out * input / (reserve_in + input)` внутри active bin. Active id не меняется, `bins_crossed=1`; `bin_step` и полноценное variable-fee состояние не определяют переходы требуемой DLMM-модели.

Это не реализация заявленных в ТЗ bin traversal/price/fee transitions. README обещает больше, чем делает код. Следует временно считать adapter неподдержанным для доказательной оценки PnL. Приёмка требует независимых fixtures для нескольких bins, смены active id, bin exhaustion, обоих направлений, exact-out и fees/post-state.

### F06 — P0: freshness/consistency snapshots нельзя считать проверенными

Три связанные проблемы:

1. `simulation/snapshots.ts:224` собирает CPMM bundle с `context_slot=max(slots)`, пустым `dependency_vector`, `source_epoch=0`, строкой SDK `latest` и безусловным `chain_consistency="validated_multi_account_snapshot"`. Максимальный slot и эта метка сами по себе не доказывают согласованность всех использованных accounts.
2. `amm_simulation/engine.py:66` проверяет deadline запроса, но не срок действия snapshot. Runtime-проба: `state_valid_until_monotonic_ns=1`, часы `10^12`, результат `complete=True`. Для offline replay игнорирование старого live TTL может быть допустимо, но replay и live должны иметь явную границу; сейчас live safety не доказана.
3. `LazySnapshotSequentialSimulator` в `backend.py:299` захватывает snapshot один раз. `WorkerSequentialSimulator` идёт через helper с default clock `lambda: 0`. В `unified_perp_analyzer.py:1998` для freshness snapshot подставляется текущее время расчёта, а не фактическое receipt time snapshot. Такой snapshot будет выглядеть свежим при повторном использовании.

Нужны настоящие dependency/version receipts, отдельные live/replay policies, TTL и invalidation по boot/epoch/generation, обновление snapshots и корректная quality classification. Просроченные или несогласованные данные не должны получать timing-valid статус.

### F07 — P0: размер perp-хеджа не привязан к локальному AMM output

В `unified_perp_analyzer.py:1639` `quantity` выводится из старой exact quote (`quote.base_amount`). После вызова нового simulator это же quantity передаётся `_evaluate_dex_perp_sequential`, где используется для perp PnL. Проверки равенства с `result.buy_output_raw` с учётом asset decimals нет. `WorkerSequentialSimulator` получает из quote лишь input amount и использует заранее заданный pool; строгая привязка quote identity к pool/assets результата также не обеспечена этим путём.

Следствие при будущей интеграции: DEX может купить другое количество base, а расчёт perp останется на старом размере. Нужны canonical asset/route binding и sizing от фактического локального output. Несовпадение lot size — либо явный отказ, либо отдельная реализованная residual-ledger модель; линейное масштабирование exact quote недопустимо.

### F08 — P1: worker lifecycle/caps/deadlines/cancel незавершены

`worker.ts:20` хранит snapshots в `Map`; для него не реализованы требуемые item/byte caps, TTL eviction и полноценная инвалидизация токенов. Capture handler на строке 312 экспортирует только CPMM через `cpmmSimulationState`, несмотря на наличие builders остальных протоколов.

`deadline_monotonic_ns` не контролирует исполнение TS simulation; `cancel_simulation` около строки 301 только подтверждает отмену сообщением. Это не доказательство прекращения работы/отбрасывания результата. При интенсивном использовании capture Map может расти; текущий малый benchmark этого не проверяет.

Приёмка: bounded registry с метриками, churn/expiry tests, restart/generation tests, deadline/cancel semantics с контролем позднего результата; поэтапное расширение capture только на проверенные протоколы.

### F09 — P1: live evidence/replay не завершён

Worker `export_simulation_evidence` возвращает snapshot, а не полный request/result/evidence bundle. В обычном `WorkerSequentialSimulator` `as_result()` вызывается без evidence hash. Offline build/save/load/replay helpers существуют и работают на fixtures, но это не тот же сценарий, что восстановление реального live-кандидата.

Приёмка: кандидат хранит стабильную ссылку/hash на неизменяемые snapshot + request + result + версии модели; replay повторяет и путь, и показанные денежные величины без сети. Нужны bounded export/retention и негативные проверки подмены/неполноты evidence.

### F10 — P1: benchmark не покрывает заявленную приёмку

`tools/benchmark-amm-simulation.ts` измеряет только маленькие CPMM пути. В блоке «cold capture» bundle строится до старта таймера. «Event-loop stall» — длительность синхронного вызова, а не измерение задержки event loop при конкурирующей нагрузке. Heap — моментальный снимок процесса, не проверка snapshot registry caps/утечек. CLMM/DLMM target только печатается; соответствующих замеров нет.

Нужны реальные bounded CLMM/DLMM workloads, cold capture с декодированием/копированием, event-loop delay instrumentation, churn и длительный soak. Текущие цифры полезны как microbenchmark CPMM и только в этом качестве.

## 7. Что ещё не готово вне AMM

- **Полноценный Asset/FX контур.** Есть строгие идентификаторы и защитные отказы, но нет универсального проверенного соответствия всех CEX/chain assets и executable FX-переходов. Сейчас несовместимые settlement currencies отбрасываются — это лучше фиктивного приравнивания USDT/USDC, но ограничивает покрытие.
- **Глубина во всех стратегиях.** CEX↔DEX analyzer умеет учитывать L2. Perp/spot-pair analyzer явно ограничен BBO и видимым размером. Нельзя переносить L2-гарантии первого на второй.
- **Positions и residual delta.** Есть lifecycle сигнала, но нет законченного жизненного цикла позиции с остатками, фактическими funding cashflows, переоценкой выхода, margin и восстановлением после сбоя.
- **Borrow / S07.** Нет подтверждённого займа, лимитов, отзывов и стоимости закрытия обязательства. Продажа имеющегося spot и short заёмного spot должны оставаться разными сценариями.
- **S09/S10.** Funding calendar helper не заменяет event capture и hedge switching. Нужны сценарии latency/eligibility, существующая позиция, стоимость перехода и baseline hold.
- **Общий поиск и размер.** Есть ограниченные заранее заданные пары/треугольники. Не приняты общий граф коротких путей, optimizer, сравнение с exhaustive oracle и совместное использование одной ликвидности несколькими кандидатами.
- **Portfolio/resource reservation.** Нет полноценного распределения капитала/маржи и ликвидности между одновременно показанными возможностями. Суммировать их PnL нельзя.
- **Partial fill / unwind / recovery.** Не завершена модель экспозиции при исполнении одной ноги и недоступности второй, стоимости аварийного хеджа и восстановления ledger.
- **Retention по общему ТЗ.** Perp journal использует `batched_append_until_count_cap`: после cap перестаёт сохранять новые terminal summaries. Политика явно сообщается, но не выполняет требование полноценной rotation/aggregation для продолжающейся работы. Cycle journal ограничен и переписывается; ресурсная стоимость долгой работы требует отдельного профилирования.
- **Acceptance/coverage.** Нельзя закрыть T01–T38 и AS01–AS42 по общему числу тестов или их названиям. Нужна поштучная связь requirement → независимый oracle → тест → production path → evidence. Модули `amm_core.py`, `amm_path_executor.py` и пакет `amm_simulation/` также требуют согласования владельцев контрактов, чтобы параллельные реализации не расходились.

## 8. Что показал предыдущий live-запуск

Последний найденный run: `all-markets-20260909-213627`. Старт 10 сентября около 00:36 МСК; последний status — **10 сентября 2026, 19:21:43 МСК**. Сохранённая длительность 67 515,86 с, то есть около **18 ч 45 мин**. На момент аудита scanner-процесс не найден.

`latest.json` по-прежнему содержит `status="running"`, но это устаревшая запись, а не свидетельство работающего процесса. Причина остановки в этом аудите не установлена. Старый запуск не проверяет новую AMM-интеграцию и не заменяет 24-часовую приёмку текущего кода.

| Сохранённый показатель | Значение |
| --- | --- |
| CEX/DEX + triangles evaluations | 1 178 730 |
| Из них timing-valid | 387 836 |
| Timing-valid positive после minimum network reserve | 4 401 |
| Cycle candidate lifecycle starts / closes | 3 209 / 3 209 |
| Perp/spot-family strategy evaluations | 166 515 793 |
| Из них timing-valid | 10 396 046 |
| Timing-valid positive после модельных расходов | 1 632 |
| Matched DEX reverse exact-quote pairs | 397 |
| DEX/perp exact quantity mismatch | 12 013 000 |
| Perp-family candidate lifecycle starts / closes | 1 278 / 1 278 |
| Сохранённые perp terminal summaries | 21 |

Это повторные оценки на обновлениях и lifecycle-события, **не число сделок и не число независимых доступных возможностей**. Большая доля timing/FX/quantity отказов показывает, что сбор работает, но практический охват полных моделей заметно уже числа полученных цен.

Особенно важно: в cycle top сохранён BONK/Jupiter/Bybit результат около **54 001 bps**. Его причина не диагностирована. Нельзя считать такой выброс доказанной прибылью; нужен разбор asset identity/units, состояния CEX-книги, совместимости размеров и исходной DEX quote. Одной проверки локального времени недостаточно. Отсутствие полных raw snapshots ограничивает ретроспективную проверку.

Другой пример: лучший сохранённый spot/spot HMSTR edge около **473,8 bps** рассчитан на модельный notional лишь **1,47 USDT**, с эффектом около **0,07 USDT**, а не на условные 100 USDT из reference bucket. Это показывает, почему bps без исполнимого размера вводит в заблуждение.

## 9. Рекомендуемый порядок следующей работы

1. **Зафиксировать честный capability baseline.** Исправить README/матрицу приёмки, неподтверждённые CLMM/DLMM держать unsupported для точного PnL; основной scanner оставить на прежней аналитике. Отдельно расследовать исторический BONK-выброс.
2. **Закрыть один CPMM вертикальный сценарий.** Единый Broker API, initial balances в JSONL, корректный snapshot capture/quality/TTL/generation, binding pool/assets и hedge quantity, evidence. Затем подключить через feature flag в реальном composition root.
3. **Принять этот сценарий end-to-end.** Unit + independent fixtures + настоящий worker protocol test + composition-root test + ограниченный read-only live smoke. Должен появиться отдельный sequential shadow result, воспроизводимый offline; старые paired rows не должны получить повышенный статус.
4. **Добавить Orca как вторую независимую модель.** Реальный capture и та же end-to-end приёмка, а не только pure math. После этого расширять Stage C через точную CLMM/DLMM математику и audited AMM v4 subset.
5. **Параллельно по архитектурному плану, но не путём выдачи лишних live-кандидатов:** position/residual/borrow/funding eligibility, затем S09/S10, FX и portfolio resources. Все используют один cost/ledger contract.
6. **Завершить M3/M5/M6.** Общие маршруты и optimizer; reservation/partial fills/recovery; rotation и decision replay; измеренный нагрузочный профиль, fault injection и минимум 24 ч soak текущей версии.

Критерий полезного ближайшего результата: не «появилось больше положительных bps», а **одна точная, свежая, количественно согласованная DEX↔perp модель с проверяемым post-state и воспроизводимым evidence**. Корректное заключение «после расходов возможностей нет» полностью допустимо.

## 10. Итог для запуска

**Можно:** запускать существующий read-only scanner для оценки покрытия, свежести, отказов, модельных spot/spot, CEX/DEX, ограниченных triangles и spot/perp/perp-pair сценариев; запускать offline тесты и AMM fixture replay.

**Пока нельзя заявлять:** готовый полный релиз S01–S10, точную live CLMM/DLMM post-state оценку, работающий по одному флагу sequential AMM-контур, полноценный виртуальный портфель и воспроизводимость каждого live-кандидата.

**Главный следующий этап:** закончить и принять M2 на одном сквозном CPMM-сценарии, затем втором Orca-сценарии. Сборщик уже приносит полезные данные; слой, превращающий их в доказательные выводы о стратегии и капитале, ещё требует существенной работы.
