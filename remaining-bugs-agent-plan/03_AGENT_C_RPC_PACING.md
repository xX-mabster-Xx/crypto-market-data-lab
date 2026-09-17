# Агент C — pacing каждого физического RPC request

**Модель: OpenAI GPT-5.6 Sol. Reasoning: high.**
Выбор: nested async context, ограничение очередей и риск deadlock при повторном admission требуют внимательного concurrency-дизайна.

**Исправить:** O-01. Связанные требования: BUG-018 и request-level invariant BUG-009.
**Старт:** волна 1 параллельно A/B. D начинает engines только после твоего handoff.

## Обязательные правила выполнения

Это задание на исправление кода, но выполнять его следует только после назначения этого задания агенту. Само наличие файла не запускает работу. Работай в репозитории crypto-market-data-lab; найди его корень, не полагайся на текущий каталог. Базовый аудит: commit `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`, файл `BUGFIX_V2_AUDIT_2026-09-16.md`. Оригинальные требования: `crypto-market-data-lab_bugfix_spec_v2.md`. Прочитай относящиеся к тебе разделы и локальные AGENTS.md. Аудит — проверяемая исходная информация, не замена чтению текущего кода: номера строк могли измениться.

Начни с `git status --short` и проверки текущей реализации. Не восстанавливай и не включай в свою работу пользовательские удаления `TECH_SPEC_NEXT_TASK_CPMM_VERTICAL_SLICE.md`, `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`, `bug-002-keyed-coalescing-event-bus.patch`. Сохраняй все чужие изменения. Не делай reset/clean, массовое форматирование, upgrade SDK, commit/push или live trading. Не изменяй исторический аудит задним числом. Не создавай субагентов автоматически.

Сначала воспроизведи дефект тестом на production-коде; затем минимально исправь и покажи, что тот же тест стал зелёным. Моки допустимы на IO/SDK/clock-границе, но не подменяй исправляемый алгоритм его тестовой копией. Проверки текста исходников не заменяют runtime-тесты. Не ослабляй ожидания и не удаляй регрессии ради зелёного результата. Используй apply_patch для ручных изменений. Генераторы lockfile и форматтеры допустимы в своём scope; соблюдай действующие approval/network правила.

Читай чужие файлы по необходимости, но редактируй только свою зону. Если нужен чужой интерфейс, зафиксируй точный контракт в handoff и согласуй передачу владения; не вноси скрытых параллельных правок. Отсутствие доступа/зависимости/Node 24 отмечай как BLOCKED, не как PASS. По умолчанию тесты offline: без реальных RPC, секретов, глобального прокси, root и изменения системного runtime.

Для экономии сначала запускай новые узкие тесты, затем относящийся к задаче пакет. Полный совместный gate принадлежит агенту G. Не считай прежние 486 Python tests / 12 Node test files гарантией исправления: они были зелёными и при найденных дефектах.

## Формат сдачи

Создай `remaining-bugs-agent-plan/results/AGENT_<ID>_RESULT.md`, подставив свою букву. Укажи: commit/dirty baseline; закрываемые audit IDs; изменённые файлы; причина дефекта; что изменено; команды и фактические результаты тестов; доказательство red→green; изменения интерфейсов/схем; совместимость; риски и BLOCKED/NOT RUN проверки. Для каждого ID — FIXED / PARTIAL / NOT FIXED с доказательством. Приложи компактные артефакты, но не бесконечные raw-event логи и не секреты. Нельзя писать «всё исправлено», если хотя бы один обязательный критерий не проверен.

## Зона владения

- `workers/solana-quote-worker/src/rpcPacer.ts`.
- Собственный вспомогательный транспорт/pacer module, если действительно нужен.
- `workers/solana-quote-worker/test/rpcScheduler.test.ts`, новые `test/remainingC*.test.ts`.
- Engine call sites читать; если контракт RunRpcJob меняется, D адаптирует их после тебя. Не редактировать engines, worker.ts или package manifests одновременно с другими агентами.

## Причина и цель

В start(job) весь job.fn запускается внутри scheduledRpcStart=true. sharedRpcFetch видит флаг и выполняет ВСЕ вложенные fetch напрямую. Одна логическая SDK operation может сделать несколько HTTP reads/retries, но scheduler считает одну request-start.

Регрессия: minimum interval200ms; один scheduleRpc job последовательно вызывает реальный sharedRpcFetch дважды; global fetch заменён только локальным recorder/response stub. До исправления запросы начинались с разницей ~15.60ms и startedTotal был1. Тест должен измерять physical fetch, а не entry в scheduler.

Раздели логическое admission/coalescing SDK jobs и физический request-start pacing так, чтобы каждый реальный fetch/retry проходил ровно один start gate. Разрешён отдельный bounded transport gate. Не достаточно заменить boolean на «всегда повторно enqueue»: при concurrency=1 родительский job может ждать child job в занятом им же scheduler и зависнуть.

Сохрани:
- hard caps, приоритет interactive > bootstrap > refresh, deadlines, cancellation, coalescing key semantics;
- отсутствие double pacing одиночного HTTP request;
- bounded число ожидающих физических requests, включая Promise.all fan-out;
- корректный accounting actual starts/inflight/queues/errors и отдельно logical jobs;
- rejection overload/timeout до реального fetch, видимость ошибки вызывающему;
- shutdown при waiting timer, queued job, active fetch; освобождение pending buffers/listeners;
- compatibility middleware: если он остаётся поддерживаемым, его поведение тоже проверяется; не обещай guarantee только одному из двух adapters.

## Обязательные тесты

1. Один logical job →2 sequential requests и →несколько concurrent requests: для отсортированных фактических стартов gap не меньше configured interval с контролируемым clock/tolerance.
2. Несколько jobs + direct sharedRpcFetch + вложенные вызовы: единый global budget, no double delay одиночного request.
3. Конкурентность logical scheduler=1: вложенный fetch завершается, deadlock отсутствует.
4. Retry transport/web3 wrapper вызывает fetch повторно: каждый attempt paced. Stub retries, без сети.
5. 100,000 coalescible refresh jobs: queue bounded, конечное актуальное выполнение, interactive получает обслуживание.
6. Per-physical-request overload, deadline before start, cancellation и close: нет зависших promises и незарегистрированных starts.
7. Пауза/ошибка fetch не сбрасывает interval и не вызывает burst. Метрики согласованы с recorder.

Инъекция deterministic monotonic clock предпочтительнее секундных real sleeps. Для одного smoke допустим real monotonic clock с разумным tolerance; не использовать Date.now для решения elapsed-time semantics.

## Приёмка C и контракт для D/F

Сдать описание, какие API планируют logical jobs, какие physical HTTP, где учитываются timeout/deadline и какие метрики читает F. По возможности сохранить внешний RunRpcJob API; если это невозможно, перечислить exact call-site changes для D и отметить migration незавершённой до их теста. `npm run check` и scheduler tests проходят; production fetch path exercised. В result используй ID C.
