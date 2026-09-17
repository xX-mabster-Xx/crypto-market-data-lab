# Агент D — Solana dependency caches, provenance и refresh fairness

**Модель: OpenAI GPT-5.6 Sol. Reasoning: high.**
Выбор: самый сложный пакет — согласованность account versions и атомарность cache updates. Эти изменения намеренно у одного владельца: дробление тех же engines между несколькими агентами увеличит риск конфликтов.

**Исправить:** R-03, R-04, O-02, O-03. Связанные требования: BUG-019/020/023; сохранить bounded mailbox BUG-017.
**Старт:** после handoff A и C; B может ещё работать. E ждёт тебя.

## Обязательные правила выполнения

Это задание на исправление кода, но выполнять его следует только после назначения этого задания агенту. Само наличие файла не запускает работу. Работай в репозитории crypto-market-data-lab; найди его корень, не полагайся на текущий каталог. Базовый аудит: commit `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`, файл `BUGFIX_V2_AUDIT_2026-09-16.md`. Оригинальные требования: `crypto-market-data-lab_bugfix_spec_v2.md`. Прочитай относящиеся к тебе разделы и локальные AGENTS.md. Аудит — проверяемая исходная информация, не замена чтению текущего кода: номера строк могли измениться.

Начни с `git status --short` и проверки текущей реализации. Не восстанавливай и не включай в свою работу пользовательские удаления `TECH_SPEC_NEXT_TASK_CPMM_VERTICAL_SLICE.md`, `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`, `bug-002-keyed-coalescing-event-bus.patch`. Сохраняй все чужие изменения. Не делай reset/clean, массовое форматирование, upgrade SDK, commit/push или live trading. Не изменяй исторический аудит задним числом. Не создавай субагентов автоматически.

Сначала воспроизведи дефект тестом на production-коде; затем минимально исправь и покажи, что тот же тест стал зелёным. Моки допустимы на IO/SDK/clock-границе, но не подменяй исправляемый алгоритм его тестовой копией. Проверки текста исходников не заменяют runtime-тесты. Не ослабляй ожидания и не удаляй регрессии ради зелёного результата. Используй apply_patch для ручных изменений. Генераторы lockfile и форматтеры допустимы в своём scope; соблюдай действующие approval/network правила.

Читай чужие файлы по необходимости, но редактируй только свою зону. Если нужен чужой интерфейс, зафиксируй точный контракт в handoff и согласуй передачу владения; не вноси скрытых параллельных правок. Отсутствие доступа/зависимости/Node 24 отмечай как BLOCKED, не как PASS. По умолчанию тесты offline: без реальных RPC, секретов, глобального прокси, root и изменения системного runtime.

Для экономии сначала запускай новые узкие тесты, затем относящийся к задаче пакет. Полный совместный gate принадлежит агенту G. Не считай прежние 486 Python tests / 12 Node test files гарантией исправления: они были зелёными и при найденных дефектах.

## Формат сдачи

Создай `remaining-bugs-agent-plan/results/AGENT_<ID>_RESULT.md`, подставив свою букву. Укажи: commit/dirty baseline; закрываемые audit IDs; изменённые файлы; причина дефекта; что изменено; команды и фактические результаты тестов; доказательство red→green; изменения интерфейсов/схем; совместимость; риски и BLOCKED/NOT RUN проверки. Для каждого ID — FIXED / PARTIAL / NOT FIXED с доказательством. Приложи компактные артефакты, но не бесконечные raw-event логи и не секреты. Нельзя писать «всё исправлено», если хотя бы один обязательный критерий не проверен.

## Зона владения

В `workers/solana-quote-worker/src/`:
- meteoraDlmm.ts, orcaWhirlpool.ts, raydiumClmm.ts, raydiumStandard.ts, engineRuntime.ts;
- минимальные engine-level helper modules;
- свои `test/remainingD*.test.ts`, engineRuntime.test.ts.
worker.ts maintenance orchestration меняй только если engine-local fairness недостаточна; это твоя очередь владения до E/F. Не менять Python admission, rpcPacer internals, snapshot codec.

## D1. R-03 + O-03: безопасный refresh зависимостей

Исходный probe: Meteora cached bin old со slot110 и активной подпиской; SDK refresh возвращает new-rpc; два refresh сохраняют old, сбрасывают cache age, увеличивают generation1→3 и делают2 notifications. Аналогичная preservePushed ветка есть в Orca.

Нужен version-aware reconciliation:
- Не считать наличие любого исторического WS slot доказательством, что WS state новее любого RPC.
- Не перезаписывать known-newer data contextless/older RPC.
- Предпочти context-bearing account read с проверенной декодировкой нужного SDK layout; исследуй фактические SDK APIs, не выдумывай response slot.
- Зафиксируй revision перед await и сравни после; если WS обогнал response, сохранить более новую account version.
- Если контекст/декодирование/согласованность не подтверждены, не помечать старую cache свежей: явный stale/unavailable/retry status по существующему контракту. В штатном режиме refresh обязан уметь восстановить пропущенный WS update, а не всегда отказываться.
- Account set add/remove, subscription lifecycle и removal provenance должны быть согласованы; unknown removals не оставляют stale accounts.
- Stage→validate→commit: quote не должен видеть частично новую cache со старым provenance между await.
- Раздели semantic content generation и successful validation/refresh timestamp. Неизменные data при более поздней подтверждённой проверке не должны создавать downstream quote-state storm. Если публикуешь provenance-only health, делай это отдельным bounded/coalesced каналом с явной семантикой.
- Не сериализуй все tick/bin arrays на каждый WS update ради fingerprint. Используй bounded per-account hashes/revisions и измерь стоимость.

Тесты для ОБОИХ engines: unchanged repeated refresh; newer context replacing stale subscribed value; older response after newer WS; same-slot duplicate policy; missing account/decode error; account set changed; no WS notification followed by successful RPC repair; pending debounce + immediate core update; shutdown timers/subscriptions. Проверить реальные quote inputs и emitted summary, не только mock requestPoolState.

## D2. R-04: CPMM config rollback

Core100/config120; RPC snapshot105 должен продвинуть допустимый core, но не заменить config120 на105 и не выдать ложный согласованный snapshot. Сейчас присваивание config происходит до проигнорированного acceptDependency(false).

Декодируй и валидируй snapshot во временные структуры. До commit проверь freshness каждой account dependency и общую policy consistency. Выбери документированную корректную policy (сохранить более новый config с честным mixed provenance либо получить действительно согласованный набор), без rollback и без необоснованного validation claim. Ошибка decode не должна продвигать provenance раньше данных. Проверь также vault dependencies и WS callbacks на тот же класс «сначала поменять данные, потом отвергнуть версию».

Production regression с реальными decoder layouts/fixtures: core100/config120 + delayed RPC105; fee quote и simulation state используют правильную fee, core105 не отвергнут из-за dependency120, metadata соответствует bytes. Также свежий RPC125, malformed config, equal slot duplicate.

## D3. O-02: starvation-free maintenance всех четырёх engines

Исходный probe:100 pools, default15s age +5s stagger,600 ticks по1s →только20 pools serviced,80 never, первый32 раза. Каждый tick начинает iteration с первого элемента и возвращает после первого due.

Введи rotating cursor / oldest-overdue selection или иной bounded fair selection; гарантируй eventual service при конечном fixed universe и доступной RPC capacity. Не обещай невозможный freshness SLA, если refresh demand выше capacity: показывай overdue/backlog, сохраняя fairness. Fresh/inflight pools пропускать; coalesced refresh не дублировать; errors/backoff не должны заставлять вечно выбирать первый failing pool. Не возвращать глобальный refresh-all burst.

Параметризованный тест по всем engines:100 pools и600 deterministic ticks; каждый due pool получает service, первые не монополизируют; добавить/удалить pool вокруг cursor; один failing/slow pool; fresh WS pools skipped; bounded refresh inflight; low-priority scheduler C и stagger сохраняются.

## D4. Контракты и приёмка

Согласуй с A monotonic dependency generation и reset при epoch. Передай E API получения immutable account data + реального core/dependency provenance + monotonic age/validation basis. Поля core_state_slot/legacy slot всегда означают core; max dependency никогда не заменяет их.

Запусти Node check, свои production-engine tests и существующие engine/simulation tests. Добавь stress100,000 updates на фиксированном числе pools: one pending core update; bounded emit count; final data/provenance верны; repeated unchanged refresh не создаёт emissions. Helper-only/source-regex тест недостаточен. В result используй ID D и все4 audit IDs.
