# Агент E — честная provenance и freshness simulation snapshots

**Модель: OpenAI GPT-5.6 Sol. Reasoning: high.**
Выбор: cross-language schema, evidence и stale-state rejection относятся к корректности результата, а не косметическому добавлению JSON полей.

**Исправить:** R-05; связанная snapshot-часть O-05. Требование BUG-020.
**Старт:** после D и его data/provenance API; учесть контракт A. F ждёт твоей передачи worker.ts и SDK metadata.

## Обязательные правила выполнения

Это задание на исправление кода, но выполнять его следует только после назначения этого задания агенту. Само наличие файла не запускает работу. Работай в репозитории crypto-market-data-lab; найди его корень, не полагайся на текущий каталог. Базовый аудит: commit `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`, файл `BUGFIX_V2_AUDIT_2026-09-16.md`. Оригинальные требования: `crypto-market-data-lab_bugfix_spec_v2.md`. Прочитай относящиеся к тебе разделы и локальные AGENTS.md. Аудит — проверяемая исходная информация, не замена чтению текущего кода: номера строк могли измениться.

Начни с `git status --short` и проверки текущей реализации. Не восстанавливай и не включай в свою работу пользовательские удаления `TECH_SPEC_NEXT_TASK_CPMM_VERTICAL_SLICE.md`, `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`, `bug-002-keyed-coalescing-event-bus.patch`. Сохраняй все чужие изменения. Не делай reset/clean, массовое форматирование, upgrade SDK, commit/push или live trading. Не изменяй исторический аудит задним числом. Не создавай субагентов автоматически.

Сначала воспроизведи дефект тестом на production-коде; затем минимально исправь и покажи, что тот же тест стал зелёным. Моки допустимы на IO/SDK/clock-границе, но не подменяй исправляемый алгоритм его тестовой копией. Проверки текста исходников не заменяют runtime-тесты. Не ослабляй ожидания и не удаляй регрессии ради зелёного результата. Используй apply_patch для ручных изменений. Генераторы lockfile и форматтеры допустимы в своём scope; соблюдай действующие approval/network правила.

Читай чужие файлы по необходимости, но редактируй только свою зону. Если нужен чужой интерфейс, зафиксируй точный контракт в handoff и согласуй передачу владения; не вноси скрытых параллельных правок. Отсутствие доступа/зависимости/Node 24 отмечай как BLOCKED, не как PASS. По умолчанию тесты offline: без реальных RPC, секретов, глобального прокси, root и изменения системного runtime.

Для экономии сначала запускай новые узкие тесты, затем относящийся к задаче пакет. Полный совместный gate принадлежит агенту G. Не считай прежние 486 Python tests / 12 Node test files гарантией исправления: они были зелёными и при найденных дефектах.

## Формат сдачи

Создай `remaining-bugs-agent-plan/results/AGENT_<ID>_RESULT.md`, подставив свою букву. Укажи: commit/dirty baseline; закрываемые audit IDs; изменённые файлы; причина дефекта; что изменено; команды и фактические результаты тестов; доказательство red→green; изменения интерфейсов/схем; совместимость; риски и BLOCKED/NOT RUN проверки. Для каждого ID — FIXED / PARTIAL / NOT FIXED с доказательством. Приложи компактные артефакты, но не бесконечные raw-event логи и не секреты. Нельзя писать «всё исправлено», если хотя бы один обязательный критерий не проверен.

## Зона владения

- `workers/solana-quote-worker/src/simulation/snapshots.ts` и связанные simulation types/codec/handler.
- `workers/solana-quote-worker/src/worker.ts`: snapshot/simulation request handling; не менять планировщик RPC.
- `workers/solana-quote-worker/src/protocol.ts`: совместимое расширение snapshot protocol.
- Реально используемые Python snapshot/evidence contracts и codec: начни с `src/market_data_lab/amm_simulation/`, затем найди actual imports; не предполагается файл models.py.
- Snapshot/parity tests и свои `test/remainingE*.test.ts`, `tests/test_remaining_e_*.py`.
- Минимальное расширение engine snapshot-export interface согласовать с D после его завершения. package.json/lock не менять: F.

## Дефект

CPMM live state core100/deps110..120/gen7 превращается в snapshot context100, dependency_vector=[], без доступных provenance summaries, с безусловным validated_multi_account_snapshot. processSnapshotRequest создаёт новый TTL на время capture, даже если market data давно не проверялись. SDK version записана как latest.

## Что сделать

1. Проследить реальный путь capture→worker response→Python decode→simulation→evidence/replay. Согласовать поля во всех звеньях; не исправлять только standalone builder.
2. Сохранить реальный per-pool core slot, dependency summaries/generation и достоверные account versions, если D их предоставляет. Summary slots не превращать в вымышленные address/owner/hash/write-version. Для отсутствующего evidence использовать явный supported-but-unverified/unavailable статус согласно доменному контракту.
3. Значение chain_consistency должно следовать выполненным проверкам. Если запрос требует stronger consistency, чем доступна, дать typed state_unavailable/unsupported reason, не ответить ok с декоративной строкой. Независимые account slots не обязаны совпадать, но их использование должно быть проверено и честно описано.
4. Freshness вычислять от реального monotonic receipt/validation данных D, а не от capture. Snapshot token lifetime ограничить минимумом configured token TTL и оставшегося срока underlying state. Capturing/export/replay не омолаживают stale data. Не сравнивать raw monotonic timestamps разных процессов/boot: передать age/remaining TTL с безопасной receipt anchoring либо использовать проверенную существующую clock contract.
5. Snapshot должен быть immutable coherent capture: никакого shared mutable SDK object. State/version coupling не рвётся через await. Boot/generation/source_epoch bindings сохраняются; старый token не валиден после reset.
6. SDK metadata брать из разрешённого installed/package metadata источника фактической версии; не latest и не вручную продублированная будущая версия. Согласовать с F чистую установку и version consistency tests.
7. Расширить schema совместимо либо явно bump version с controlled rejection/migration старых snapshots. Не молча интерпретировать старый snapshot как проверенный новый. Сохранить bit-exact AMM arithmetic и fixtures; менять quote formulas здесь не нужно.

## Обязательные тесты

- core100/deps110..120/gen7 survives Node serialization → Python decode → evidence; per-pool provenance не теряется.
- Несколько pools с разными core/dependency slots: aggregate context не притворяется полной evidence vector.
- Missing/partial account evidence не получает validated flag; запрошенная stronger consistency отказана.
- Stale underlying cache cannot obtain fresh-valid token; boundary TTL, monotonic advance и wall-clock jump.
- Fresh cache получает корректный remaining TTL; capture/export повторно не продлевают срок.
- WS update during capture: либо coherent version, либо controlled retry/unavailable; не смешанные значения.
- immutable nested data, token boot/generation mismatch, unknown schema version, legacy compatibility.
- Actual SDK version совпадает с runtime metadata.
- Existing deterministic simulation, round-trip, parity/replay tests проходят без ослабления assertions.

## Приёмка E

Один integration test обязан проверить production request path, а не только raydiumCpmmSnapshotBundle с искусственным объектом. No-network Node/Python tests; документированная schema и semantics свежести/consistency. В handoff F перечислить поля worker protocol и exact source SDK-version metadata. В result используй ID E.
