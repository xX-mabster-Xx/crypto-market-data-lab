# Агент F — worker observability и воспроизводимый runtime

**Модель: OpenAI GPT-5.6 Luna. Reasoning: medium; high для локального разбора сбоя, если доступно.**
Выбор: ограниченные schema/config изменения с полным списком полей и тестов; сложные algorithms engines/pacer уже сданы D/C.

**Исправить:** O-05, O-06. Требования BUG-014, BUG-024 и status §33.
**Старт:** после A/C/D/E. Можно заранее читать metadata, но не редактировать их общие файлы до handoff.

## Обязательные правила выполнения

Это задание на исправление кода, но выполнять его следует только после назначения этого задания агенту. Само наличие файла не запускает работу. Работай в репозитории crypto-market-data-lab; найди его корень, не полагайся на текущий каталог. Базовый аудит: commit `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`, файл `BUGFIX_V2_AUDIT_2026-09-16.md`. Оригинальные требования: `crypto-market-data-lab_bugfix_spec_v2.md`. Прочитай относящиеся к тебе разделы и локальные AGENTS.md. Аудит — проверяемая исходная информация, не замена чтению текущего кода: номера строк могли измениться.

Начни с `git status --short` и проверки текущей реализации. Не восстанавливай и не включай в свою работу пользовательские удаления `TECH_SPEC_NEXT_TASK_CPMM_VERTICAL_SLICE.md`, `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`, `bug-002-keyed-coalescing-event-bus.patch`. Сохраняй все чужие изменения. Не делай reset/clean, массовое форматирование, upgrade SDK, commit/push или live trading. Не изменяй исторический аудит задним числом. Не создавай субагентов автоматически.

Сначала воспроизведи дефект тестом на production-коде; затем минимально исправь и покажи, что тот же тест стал зелёным. Моки допустимы на IO/SDK/clock-границе, но не подменяй исправляемый алгоритм его тестовой копией. Проверки текста исходников не заменяют runtime-тесты. Не ослабляй ожидания и не удаляй регрессии ради зелёного результата. Используй apply_patch для ручных изменений. Генераторы lockfile и форматтеры допустимы в своём scope; соблюдай действующие approval/network правила.

Читай чужие файлы по необходимости, но редактируй только свою зону. Если нужен чужой интерфейс, зафиксируй точный контракт в handoff и согласуй передачу владения; не вноси скрытых параллельных правок. Отсутствие доступа/зависимости/Node 24 отмечай как BLOCKED, не как PASS. По умолчанию тесты offline: без реальных RPC, секретов, глобального прокси, root и изменения системного runtime.

Для экономии сначала запускай новые узкие тесты, затем относящийся к задаче пакет. Полный совместный gate принадлежит агенту G. Не считай прежние 486 Python tests / 12 Node test files гарантией исправления: они были зелёными и при найденных дефектах.

## Формат сдачи

Создай `remaining-bugs-agent-plan/results/AGENT_<ID>_RESULT.md`, подставив свою букву. Укажи: commit/dirty baseline; закрываемые audit IDs; изменённые файлы; причина дефекта; что изменено; команды и фактические результаты тестов; доказательство red→green; изменения интерфейсов/схем; совместимость; риски и BLOCKED/NOT RUN проверки. Для каждого ID — FIXED / PARTIAL / NOT FIXED с доказательством. Приложи компактные артефакты, но не бесконечные raw-event логи и не секреты. Нельзя писать «всё исправлено», если хотя бы один обязательный критерий не проверен.

## Зона владения

- `workers/solana-quote-worker/package.json`, package-lock.json, scoped runtime/version docs/config.
- Worker stats module, worker.ts lifecycle/emission, protocol stats type.
- `src/market_data_lab/solana_quote_worker.py` и metrics branch `solana_realtime_scanner.py`, status integration по фактическому composition path.
- Свои `test/remainingF*.test.ts`, `tests/test_remaining_f_*.py`, краткая эксплуатационная документация.
- Engine/pacer algorithms не менять. Если нужной метрики нет — запросить accessor у владельца или добавить read-only accessor без смены semantics.

## F1. O-05: package/runtime согласованность

Сейчас package.json:Node>=24<25 и exact versions; lock root:Node>=22, latest/ranges. Аудит запускался на Node26.7.0, не на поддерживаемой24.

1. Проверить фактические exact resolved версии lock/installed dependencies и ограничения engines.
2. Сохранить Node24 policy и текущие намеренно pinned версии; регенерировать согласованный lock штатным npm без незапрошенного массового upgrade. Проверить diff всего lockfile, не только его root.
3. Если exact pins несовместимы с Node24 — показать конкретное противоречие и минимальный вариант решения, согласовать изменение runtime/dependency policy; не расширять engines на26 просто ради зелёного local run.
4. Проверить чистую установку Node24 в отдельном project-scoped/temp окружении: не удалять рабочие node_modules и не менять system Node. Network/install approvals по действующим правилам; если runtime/сеть недоступны — BLOCKED с точной необходимой операцией.
5. Проверить npm ci, npm run check, npm test под Node24 и записать node/npm versions. Не считать существующий node_modules чистой установкой.
6. SDK metadata E соответствует реально установленной версии. Проверять все hard-coded latest в relevant snapshot builders/docs; не переписывать исторические отчёты. Документировать воспроизводимую команду, version source и engine policy.

## F2. O-06: worker_stats end-to-end

Добавь compact periodic message не чаще раза в5–10s (предпочтительно10s), configurable/status-request при существующем протоколе. Один stable coalescing key в backpressure-aware writer; stats не lossless unbounded stream.

Минимальные поля из спецификации:
- rss_bytes, heap_total_bytes, heap_used_bytes, external_bytes, array_buffers_bytes, uptime_seconds;
- stdout_blocked, stdout_lossless_queue_size, stdout_state_pending_keys, stdout_state_coalesced_total;
- rpc_queue_total, rpc_queue_interactive, rpc_queue_bootstrap, rpc_queue_refresh, rpc_active, rpc_queue_high_watermark;
- pool_counts_by_protocol, refresh_inflight_by_protocol, coalesced_core_updates_by_protocol, external_pool_state_emits_total.

Используй process.memoryUsage(), не forced GC. Обозначь units/gauge/counter, boot/epoch reset semantics. Новые C logical/physical RPC metrics должны быть раздельны и не выдавать job count за HTTP count. Clock-based uptime/warning duration считать monotonic.

Python wrapper хранит один latest validated stats object на worker, сбрасывает stale instance при restart и выводит в реальный persisted/console status. Не хранить бесконечный series/full messages в rolling history; по необходимости отдельно bounded diagnostic summary. Старый worker без stats не должен падать: explicit unavailable, не фиктивные нули. Невалидные types, NaN, negative counts обрабатывай явно; не путай bool с int.

Rate-limited stderr/status warnings:
- lossless stdout queue >75% cap;
- RPC queue >75% cap;
- stdout continuously blocked дольше configurable threshold.
Отправка warning не должна зависеть от заблокированного stdout и сама разрастаться; reset/recovery/cooldown корректны. Удалить stats timers/listeners при close/configure restart. Не раскрывать URLs с credentials или raw account payload.

## Обязательные тесты и приёмка F

- Required fields/types/units, total=сумма очередей по корректной C semantics.
- Fake memory sampler и scheduler/writer counters; реальные getters, не постоянные fake значения.
- 100,000 stats updates при blocked writer →one latest stable key; Python latest cardinality=1.
- Реальный worker message →wrapper →persisted status содержит memory и queue high-watermarks.
- Queue thresholds, warning rate limit, continuous blocked duration и recovery; no stdout-dependent warning deadlock.
- Restart/close не оставляют timers и старые stats.
- Legacy absent/malformed stats cases.
- Node24 clean install +check/tests с доказательством либо явно BLOCKED.

В result используй ID F. Не выдавай отсутствие metrics за обнаруженную утечку, и не называй lock metadata drift доказанным npm ci failure.
