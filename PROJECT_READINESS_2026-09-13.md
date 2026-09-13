# Оценка готовности проекта — 2026-09-13

## Итог

Проект уже пригоден для **read-only исследовательского сканирования** публичных CEX/perpetual/DEX quote данных и для офлайн-проверки CPMM-симулятора. Он не готов к production-торговле и не должен трактоваться как торговый исполнитель.

Оценка по слоям:

| Слой | Состояние | Вывод |
| --- | --- | --- |
| Детеминированное ядро Cost/Quote/AMM | готово для офлайн research | Python и TypeScript тесты зелёные |
| Raydium CPMM vertical slice | частично готов, shadow-only | есть exact-in/out и post-state; live chain evidence пока недостаточно |
| Live market scanner | запускаем | источники и анализаторы работают, но часть Solana/Drift недоступна из-за RPC 429 |
| Полный план M0–M6 | не завершён | отсутствуют portfolio/position/replay/soak/полная route-оптимизация |
| Реальное исполнение | отсутствует намеренно | нет order/wallet/transfer/transaction capability |

## Проверяемые результаты

- Python: `413` тестов, все прошли (`unittest discover`).
- TypeScript worker: `npm run check` и `npm test`: `9` test files, все прошли.
- `py_compile` для scanner и модулей AMM проходит.
- Офлайн CPMM smoke: integer exact-in `100 → 90`, обратный swap `90 → 99`; evidence hash сохраняется и воспроизводится replay.
- Просроченный snapshot отклоняется (`state_unavailable`), simulation не маскирует stale state.
- Есть bounded registry/cancellation/deadline, initial balances, source epoch, evidence hash/save/load/replay. Результат принудительно остаётся `candidate_eligible=false` и `execution_ready=false`.

## Что уже можно запускать

Обычный scanner можно запускать в read-only режиме. Он публикует:

- live CEX spot books, perpetual BBO/context и exact-quote observations;
- direct inventory и CEX–DEX–CEX triangle модели;
- spot↔spot, perp↔perp, spot↔perp и DEX↔perp исследовательские модели;
- timing/freshness, settlement/contract/fee blockers и bounded candidate lifecycle;
- `status.json`, `cycle_analysis/stats.json`, `perp_analysis/stats.json`, `capabilities.json` и ограниченные terminal candidate summaries.

Последний доступный run `all-markets-20260912-141557` на момент проверки имел свежий status (`2026-09-13T16:23Z`), `status=running`, около `94,080 s` wall time, без raw tick persistence и без calculation errors. В perp analyzer было около `401.5M` evaluations, `59.9M` timing-valid, `12,349` timing-valid positive modeled evaluations; это **не реализованная прибыль**. В cycle analyzer — около `2.47M` evaluations и `255` timing-valid positive after network-floor rows. Все такие значения являются публичными моделями и не учитывают account fees, collateral, liquidation, borrow, rebalance, withdrawals, gas и multi-leg fill risk.

## Что реально работает в CPMM slice

При явном включении конфигурации принимается ровно один allowlisted `raydium_cpmm` pool. Composition root создаёт `LazySnapshotSequentialSimulator`, жизненный цикл snapshot не блокирует основной event loop, а analyzer проверяет asset identity, quantity, perp symbol, source epoch, snapshot generation и dual-clock freshness.

Офлайн Python/TS adapters симулируют exact-in/exact-out, повторные swaps видят virtual post-state, наблюдаемый snapshot не мутируется. Поддерживаются доказуемые synthetic и Raydium CPMM fixtures.

Это всё ещё shadow/research режим: live snapshot builder сообщает `dependency_vector=[]` и использует worker SDK label `latest`; нет криптографического доказательства полного согласованного multi-account capture для production-grade chain state. В текущем composition root `QuoteBroker` создаётся с `backends={}` — наблюдения кэшируются, но AMM backend не становится полноценным live remote provider. Legacy `simulate_local_path` и новый typed `simulate_amm_path` теперь раздельны, но это не превращает результат в executable quote.

## Что сейчас ограничивает live run

В последнем статусе:

- `solana:local-exact-pools` остановлен после повторяющегося `429 : max usage reached`, updates `0`;
- `perp:drift` остановлен с ошибкой запуска worker (`Drift worker is already started`);
- CEX и большинство public exact-quote sources продолжают работать, но restart counters и возраст отдельных quote observations требуют контроля свежести.

Поэтому запуск полезен для CEX/perp/публичных quote моделей, но не является подтверждением работоспособности live CPMM path на Solana. Scanner не размещает заявки и не меняет кошельки.

## Что не готово по плану

1. **M0–M2:** полноценный versioned source registry/epoch recovery, production Cost Engine для всех venue types, QuoteBroker с реальными локальными backend registrations и строгим end-to-end bridge результата в каждом consumer.
2. **AMM:** live chain dependency proofs, account-level capture/replay, production-grade state consistency; CLMM/DLMM/AMMv4 остаются pending/ограниченными. README заявляет их как Supported, но worker явно помечает эти варианты `unsupported_pending_adapter`, а Python CLMM/DLMM код содержит `Simplified` subset.
3. **M3:** полный route search/optimizer S01–S05, residual/partial-fill ledger и post-state route composition.
4. **M4:** virtual position lifecycle S06–S10: borrow eligibility, collateral/margin, funding history/side semantics, hedge switching and unwind.
5. **M5:** portfolio reservations, concurrency, partial execution/unwind, EvidenceBundle/ledger and crash recovery.
6. **M6:** replay corpus T01–T38, fault injection, benchmark with real worker/event-loop measurements, 24–30h soak, coverage and operator/recovery documentation.
7. **Не решено системно:** FX between settlements, account-specific fees, inventory/rebalance, borrow, liquidation, gas/withdrawal costs and exchange-specific execution semantics.

## Приоритет следующих работ

1. Исправить RPC/worker availability и провести bounded live CPMM capture на одном тестовом pool без включения торговли.
2. Добавить dependency vector/account hashes и доказуемую same-slot consistency; сохранить replayable evidence.
3. Закрыть CPMM end-to-end: worker snapshot → Python result → analyzer row → stats/evidence, с реальным bounded timeout/cancel test.
4. Только после этого расширять Orca, затем CLMM/DLMM/AMMv4; не считать текущую таблицу README доказательством готовности.
5. Затем реализовать positions/portfolio/replay/soak из M3–M6.

## Безопасность запуска

Сейчас безопасны offline tests/replay и read-only scanner. Включать `amm_simulation.enabled` можно только для shadow CPMM после проверки конкретного RPC/pool и сохранения evidence. Нельзя считать положительные `net_pnl` или `positive_after_modeled_costs` разрешением на торговлю: execution layer, wallets, orders и transactions в проекте отсутствуют.
