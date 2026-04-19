GROUPS_PAGE_MARKUP = """</head>
<body>
  <div class="app-shell">
    <header class="topbar">
      <h1 class="topbar-title">NaimTech | Распределение</h1>
    </header>

    <aside class="sidebar" aria-label="Меню разделов">
      <p class="menu-label">Меню</p>
      <ul class="menu-list">
        <li>
          <button class="menu-button is-active" type="button" data-view="overview">
            <span class="menu-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24">
                <rect x="4" y="4" width="6" height="6" rx="1"></rect>
                <rect x="14" y="4" width="6" height="6" rx="1"></rect>
                <rect x="4" y="14" width="6" height="6" rx="1"></rect>
                <rect x="14" y="14" width="6" height="6" rx="1"></rect>
              </svg>
            </span>
            <span>Общая информация</span>
          </button>
        </li>
        <li>
          <button class="menu-button" type="button" data-view="distribution">
            <span class="menu-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24">
                <polyline points="4 14 9 9 13 13 20 6"></polyline>
                <polyline points="16 6 20 6 20 10"></polyline>
                <path d="M4 4h5v5H4z"></path>
              </svg>
            </span>
            <span>Распределение сделок</span>
          </button>
        </li>
        <li>
          <button class="menu-button" type="button" data-view="stats">
            <span class="menu-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24">
                <path d="M12 20a8 8 0 1 0-8-8"></path>
                <path d="M12 16a4 4 0 1 0-4-4"></path>
                <circle cx="12" cy="12" r="1.5" fill="currentColor" stroke="none"></circle>
              </svg>
            </span>
            <span>Статистика</span>
          </button>
        </li>
        <li>
          <button class="menu-button" type="button" data-view="settings">
            <span class="menu-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24">
                <circle cx="12" cy="12" r="3.25"></circle>
                <path d="M19.4 15a1 1 0 0 0 .2 1.1l.1.1a2 2 0 0 1-2.8 2.8l-.1-.1a1 1 0 0 0-1.1-.2 1 1 0 0 0-.6.9V20a2 2 0 0 1-4 0v-.2a1 1 0 0 0-.6-.9 1 1 0 0 0-1.1.2l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1 1 0 0 0 .2-1.1 1 1 0 0 0-.9-.6H4a2 2 0 0 1 0-4h.2a1 1 0 0 0 .9-.6 1 1 0 0 0-.2-1.1l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1 1 0 0 0 1.1.2h.1a1 1 0 0 0 .6-.9V4a2 2 0 0 1 4 0v.2a1 1 0 0 0 .6.9h.1a1 1 0 0 0 1.1-.2l.1-.1a2 2 0 0 1 2.8 2.8l-.1.1a1 1 0 0 0-.2 1.1v.1a1 1 0 0 0 .9.6H20a2 2 0 0 1 0 4h-.2a1 1 0 0 0-.9.6z"></path>
              </svg>
            </span>
            <span>Настройки</span>
          </button>
        </li>
      </ul>
    </aside>

    <main class="canvas">
      <section class="canvas-card" aria-live="polite">
        <div class="section-panel" id="defaultPanel">
          <span class="section-badge" id="sectionBadge">Раздел</span>
          <h2 class="canvas-title" id="sectionTitle">Общая информация</h2>
          <p class="canvas-subtitle" id="sectionDescription">
            Собираем общую информацию. Пока собрали только то, что её нет.
          </p>
        </div>

        <div class="section-panel" id="statsPanel" hidden>
          <div class="stats-view">
            <div class="distribution-reference-head">
              <h2 class="distribution-reference-title">Статистика</h2>
              <p class="distribution-reference-description">
                Здесь отображаются последние runtime-данные по распределению сделок: сводка по назначениям, журнал обработанных сделок и технические записи для диагностики.
              </p>
            </div>

            <div class="stats-toolbar">
              <button class="secondary-action" id="runDeliveryCheckButton" type="button">Проверить доставку</button>
              <button class="secondary-action" id="refreshStatsButton" type="button">Обновить журнал</button>
            </div>

            <div class="reference-status" id="statsStatus" hidden>
              Откройте раздел, чтобы загрузить журнал.
            </div>

            <section class="reference-card" aria-labelledby="statsSummaryTitle">
              <div class="reference-card-head">
                <h3 class="reference-card-title" id="statsSummaryTitle">Сводка</h3>
                <p class="reference-card-description">Только ключевые показатели по последним записям runtime-журнала.</p>
              </div>
              <div class="stats-summary-grid" id="statsSummaryList"></div>
            </section>

            <div class="stats-grid stats-grid--distribution">
              <section class="reference-card" aria-labelledby="statsDistributionTitle">
                <div class="reference-card-head">
                  <h3 class="reference-card-title" id="statsDistributionTitle">Распределение по менеджерам</h3>
                  <p class="reference-card-description">Назначения по текущей группе распределения на основе последних записей runtime-журнала.</p>
                </div>
                <div class="stats-table-wrap">
                  <table class="stats-table" aria-describedby="statsDistributionTitle">
                    <thead>
                      <tr>
                        <th scope="col">Менеджер</th>
                        <th scope="col">Группа</th>
                        <th scope="col">Сделок</th>
                        <th scope="col">Последняя сделка</th>
                        <th scope="col">Последнее назначение</th>
                      </tr>
                    </thead>
                    <tbody id="statsDistributionTableBody"></tbody>
                    <tfoot id="statsDistributionTableFoot"></tfoot>
                  </table>
                </div>
              </section>

              <section class="reference-card" aria-labelledby="statsJournalTitle">
                <div class="reference-card-head">
                  <h3 class="reference-card-title" id="statsJournalTitle">Журнал сделок</h3>
                  <p class="reference-card-description">Последние записи из runtime-журнала по сделкам с текущим статусом обработки.</p>
                </div>
                <div class="stats-scroll-area stats-scroll-area--journal">
                  <div class="reference-list" id="statsJournalList"></div>
                </div>
              </section>
            </div>

            <div class="stats-technical-stack">
              <details class="stats-disclosure">
                <summary class="stats-disclosure-summary">
                  <span>Runtime участников</span>
                  <span class="stats-disclosure-badge" id="statsMembersCount">0</span>
                </summary>
                <div class="stats-disclosure-content">
                  <p class="reference-card-description">
                    Служебные runtime-данные по последнему назначению для каждого участника.
                  </p>
                  <div class="stats-scroll-area stats-scroll-area--members">
                    <div class="reference-list" id="statsMembersList"></div>
                  </div>
                </div>
              </details>

              <details class="stats-disclosure">
                <summary class="stats-disclosure-summary">
                  <span>Диагностический журнал</span>
                  <span class="stats-disclosure-badge" id="statsDiagnosticsCount">0</span>
                </summary>
                <div class="stats-disclosure-content">
                  <p class="reference-card-description">
                    Сохраненные backend-события: bind, hit на endpoint, ошибки и причины остановки.
                  </p>
                  <div class="stats-scroll-area stats-scroll-area--diagnostics">
                    <div class="reference-list" id="statsDiagnosticsList"></div>
                  </div>
                </div>
              </details>
            </div>
          </div>
        </div>

        <div class="section-panel" id="distributionPanel" hidden>
          <div class="distribution-reference-view">
            <div class="distribution-reference-head">
              <h2 class="distribution-reference-title">Распределение сделок</h2>
              <p class="distribution-reference-description" hidden></p>
            </div>

            <div class="distribution-groups-panel" id="distributionGroupsPanel">
              <div class="distribution-groups-list" id="distributionGroupsList"></div>
              <button class="distribution-create-card" id="createDistributionGroupButton" type="button">
                <span class="distribution-create-plus" aria-hidden="true">+</span>
                <span class="distribution-create-label">Добавить новую группу</span>
              </button>
            </div>

            <div class="reference-status" id="distributionStatus" hidden>
              Откройте раздел, чтобы загрузить конфигурацию группы.
            </div>

            <form class="distribution-form" id="distributionForm" hidden>
              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Наименование группы распределения:</span>
                <span class="distribution-row-control">
                  <input class="distribution-input" id="groupNameInput" type="text" placeholder="Например, Основная группа распределения">
                </span>
              </label>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Тип распределения:</span>
                <span class="distribution-row-control">
                  <select class="distribution-select" id="distributionTypeSelect">
                    <option value="round_robin_load_time">По очереди с лимитами по нагрузке и времени</option>
                  </select>
                </span>
              </label>

              <section class="distribution-row" aria-label="Участники распределения">
                <div class="distribution-row-label">Участники распределения:</div>
                <div class="distribution-row-control">
                  <div class="participants-toolbar">
                    <span class="distribution-inline-label">Массовое заполнение лимита для:</span>
                    <select class="distribution-select" id="bulkLimitScopeSelect">
                      <option value="selected">Выбранных</option>
                      <option value="all">Всех</option>
                    </select>
                    <span class="distribution-inline-label">Лимит:</span>
                    <input class="distribution-input distribution-input--compact" id="bulkLimitValueInput" type="number" min="0" step="1" placeholder="0">
                    <button class="secondary-action" id="applyBulkLimitButton" type="button">Применить</button>
                  </div>
                  <div class="distribution-scroll-box distribution-scroll-box--participants">
                    <div class="participant-list" id="participantsList"></div>
                  </div>
                </div>
              </section>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Отслеживаемое событие:</span>
                <span class="distribution-row-control">
                  <select class="distribution-select" id="eventTypeSelect">
                    <option value="deal_created">Создание новой сделки</option>
                  </select>
                </span>
              </label>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Статус для распределения:</span>
                <span class="distribution-row-control">
                  <select class="distribution-select" id="distributionStageSelect"></select>
                </span>
              </label>

              <section class="distribution-row" aria-labelledby="loadStagesTitle">
                <div class="distribution-row-label" id="loadStagesTitle">Статусы для определения нагрузки:</div>
                <div class="distribution-row-control">
                  <div class="load-stages-layout">
                    <div class="load-stages-note">
                      <p class="distribution-section-description">
                        Вы выбрали режим распределения по нагрузке на менеджера, поэтому в данном поле следует указать, в каких
                        стадиях активные сделки являются нагрузкой для выбранных менеджеров.
                      </p>
                    </div>
                    <div class="distribution-scroll-box distribution-scroll-box--stages">
                      <div class="checkbox-list" id="loadStagesList"></div>
                    </div>
                  </div>
                </div>
              </section>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Поле для установки/определения ответственного:</span>
                <span class="distribution-row-control">
                  <select class="distribution-select" id="responsibleFieldSelect"></select>
                </span>
              </label>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Время ожидания, сек</span>
                <span class="distribution-row-control">
                  <input class="distribution-input" id="waitSecondsInput" type="number" min="1" step="1" placeholder="0">
                </span>
              </label>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Интервал перепроверки при переполнении лимитов:</span>
                <span class="distribution-row-control">
                  <input class="distribution-input" id="retryIntervalInput" type="number" min="1" step="1" placeholder="Например, 30">
                </span>
              </label>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Группа активна:</span>
                <span class="distribution-row-control">
                  <span class="distribution-checkbox-row">
                    <input id="groupActiveInput" type="checkbox">
                    <span>Активна</span>
                  </span>
                </span>
              </label>

              <div class="distribution-actions">
                <button class="primary-action" id="saveDistributionButton" type="button">Применить</button>
              </div>
            </form>
          </div>
        </div>
      </section>
    </main>
  </div>
"""
