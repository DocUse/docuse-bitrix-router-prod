from __future__ import annotations

import json


def render_blank_page(*, initial_member_id: str | None = None) -> str:
    initial_member_id_json = json.dumps(initial_member_id or "", ensure_ascii=False)
    template = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NaimTech | Распределение</title>
  <script src="//api.bitrix24.tech/api/v1/"></script>
  <style>
    :root {
      color-scheme: light;
      --brand-blue: #2e7bf4;
      --menu-bg: #eaf1fb;
      --menu-text: #5c6d96;
      --canvas-text: #1f2a44;
      --canvas-subtle: #8a96b2;
      --border-soft: #dce6f5;
    }

    * {
      box-sizing: border-box;
    }

    html, body {
      margin: 0;
      min-height: 100vh;
      background: #ffffff;
      font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    body {
      color: var(--canvas-text);
    }

    .app-shell {
      min-height: 100vh;
      display: grid;
      grid-template-columns: 287px minmax(0, 1fr);
      grid-template-rows: 93px minmax(0, 1fr);
      grid-template-areas:
        "header header"
        "sidebar main";
    }

    .topbar {
      grid-area: header;
      display: flex;
      align-items: center;
      padding: 0 32px;
      background: var(--brand-blue);
      color: #ffffff;
    }

    .topbar-title {
      margin: 0;
      font-size: 30px;
      font-weight: 700;
      line-height: 1.2;
    }

    .sidebar {
      grid-area: sidebar;
      background: var(--menu-bg);
      padding: 46px 0 32px 20px;
      border-right: 1px solid rgba(46, 123, 244, 0.06);
    }

    .menu-label {
      margin: 0 0 14px 14px;
      font-size: 14px;
      line-height: 1.2;
      text-transform: uppercase;
      color: var(--menu-text);
      letter-spacing: 0.02em;
    }

    .menu-list {
      display: flex;
      flex-direction: column;
      gap: 12px;
      padding: 0;
      margin: 0;
      list-style: none;
    }

    .menu-button {
      width: 100%;
      min-height: 42px;
      border: 0;
      background: transparent;
      color: var(--menu-text);
      border-radius: 10px 0 0 10px;
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 9px 14px 9px 10px;
      text-align: left;
      font: inherit;
      font-size: 18px;
      font-weight: 500;
      cursor: pointer;
      transition: background-color 0.15s ease, color 0.15s ease;
    }

    .menu-button:hover {
      background: rgba(46, 123, 244, 0.08);
    }

    .menu-button.is-active {
      background: var(--brand-blue);
      color: #ffffff;
    }

    .menu-icon {
      width: 24px;
      height: 24px;
      flex: 0 0 24px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }

    .menu-icon svg {
      width: 24px;
      height: 24px;
      stroke: currentColor;
      fill: none;
      stroke-width: 1.8;
      stroke-linecap: round;
      stroke-linejoin: round;
    }

    .canvas {
      grid-area: main;
      background: #ffffff;
      padding: 40px 48px;
    }

    .canvas-card {
      min-height: calc(100vh - 173px);
      border: 1px dashed var(--border-soft);
      border-radius: 20px;
      padding: 32px 36px;
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), #ffffff);
      display: flex;
      flex-direction: column;
      justify-content: flex-start;
    }

    .canvas-card.is-centered {
      align-items: center;
      justify-content: center;
      text-align: center;
      border-style: solid;
      border-color: transparent;
      background: #ffffff;
    }

    .distribution-empty-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      text-align: center;
      min-height: 100%;
    }

    .distribution-reference-view {
      display: flex;
      flex-direction: column;
      gap: 24px;
      min-height: 100%;
    }

    .distribution-reference-head {
      display: flex;
      flex-direction: column;
      gap: 10px;
    }

    .distribution-reference-title {
      margin: 0;
      font-size: 28px;
      line-height: 1.25;
      font-weight: 700;
      color: #1f2a44;
    }

    .distribution-reference-description {
      margin: 0;
      max-width: 880px;
      font-size: 15px;
      line-height: 1.6;
      color: var(--canvas-subtle);
    }

    .reference-status {
      padding: 14px 16px;
      border-radius: 14px;
      border: 1px solid var(--border-soft);
      background: #f8fbff;
      color: #39507c;
      font-size: 14px;
      line-height: 1.5;
    }

    .reference-status.is-error {
      border-color: rgba(214, 69, 69, 0.2);
      background: #fff5f5;
      color: #a83a3a;
    }

    .reference-status.is-success {
      border-color: rgba(46, 123, 244, 0.16);
      background: rgba(46, 123, 244, 0.06);
      color: #255ec0;
    }

    .reference-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
    }

    .reference-grid[hidden] {
      display: none;
    }

    .reference-card {
      min-width: 0;
      border: 1px solid var(--border-soft);
      border-radius: 18px;
      background: #ffffff;
      padding: 20px;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .reference-card-head {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }

    .reference-card-title {
      margin: 0;
      font-size: 20px;
      line-height: 1.3;
      font-weight: 700;
      color: #1f2a44;
    }

    .reference-card-description {
      margin: 0;
      font-size: 14px;
      line-height: 1.5;
      color: var(--canvas-subtle);
    }

    .reference-list {
      display: flex;
      flex-direction: column;
      gap: 10px;
      padding: 0;
      margin: 0;
      list-style: none;
    }

    .reference-item {
      display: flex;
      flex-direction: column;
      gap: 4px;
      padding: 12px 14px;
      border-radius: 14px;
      background: #f7faff;
      border: 1px solid rgba(46, 123, 244, 0.08);
    }

    .reference-item-title {
      font-size: 15px;
      line-height: 1.4;
      font-weight: 600;
      color: #24324f;
      word-break: break-word;
    }

    .reference-item-meta {
      font-size: 13px;
      line-height: 1.5;
      color: #7081a8;
      word-break: break-word;
    }

    .reference-pill-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }

    .reference-pill {
      display: inline-flex;
      align-items: center;
      padding: 4px 8px;
      border-radius: 999px;
      background: rgba(46, 123, 244, 0.08);
      color: var(--brand-blue);
      font-size: 12px;
      line-height: 1.3;
      font-weight: 600;
    }

    .reference-pill.is-muted {
      background: rgba(92, 109, 150, 0.08);
      color: #5c6d96;
    }

    .canvas-title {
      margin: 0;
      font-size: 32px;
      line-height: 1.2;
      font-weight: 700;
    }

    .canvas-subtitle {
      margin: 12px 0 0;
      max-width: 620px;
      font-size: 16px;
      line-height: 1.6;
      color: var(--canvas-subtle);
    }

    .section-badge {
      display: inline-flex;
      align-items: center;
      width: fit-content;
      margin-top: 24px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(46, 123, 244, 0.08);
      color: var(--brand-blue);
      font-size: 13px;
      font-weight: 600;
      letter-spacing: 0.01em;
    }

    .distribution-empty-title {
      margin: 0 0 18px;
      font-size: 24px;
      line-height: 1.3;
      font-weight: 700;
      color: #333333;
    }

    .primary-action {
      min-width: 226px;
      height: 42px;
      border: 0;
      border-radius: 10px;
      background: var(--brand-blue);
      color: #ffffff;
      font: inherit;
      font-size: 18px;
      font-weight: 500;
      cursor: pointer;
      transition: opacity 0.15s ease;
    }

    .primary-action:hover {
      opacity: 0.92;
    }

    .section-panel[hidden] {
      display: none;
    }

    @media (max-width: 900px) {
      .app-shell {
        grid-template-columns: 1fr;
        grid-template-rows: 93px auto minmax(0, 1fr);
        grid-template-areas:
          "header"
          "sidebar"
          "main";
      }

      .sidebar {
        padding: 24px 20px;
      }

      .menu-button {
        border-radius: 10px;
      }

      .canvas {
        padding: 24px 20px 32px;
      }

      .canvas-card {
        min-height: 360px;
      }

      .reference-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
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

        <div class="section-panel" id="distributionPanel" hidden>
          <div class="distribution-reference-view">
            <div class="distribution-reference-head">
              <h2 class="distribution-reference-title">Справочники Bitrix24 для группы распределения</h2>
              <p class="distribution-reference-description">
                На этом этапе раздел только читает данные установленного портала: сотрудников, стадии сделок и поля,
                которые подходят для установки ответственного. Сохранение группы и логика распределения будут добавлены
                следующим шагом.
              </p>
            </div>

            <div class="reference-status" id="distributionStatus">
              Откройте раздел, чтобы загрузить справочники портала.
            </div>

            <div class="reference-grid" id="distributionReferenceGrid" hidden>
              <section class="reference-card" aria-labelledby="usersCardTitle">
                <div class="reference-card-head">
                  <h3 class="reference-card-title" id="usersCardTitle">Участники распределения</h3>
                  <p class="reference-card-description">Сотрудники Bitrix24, доступные для будущего выбора в группе.</p>
                </div>
                <ul class="reference-list" id="usersList"></ul>
              </section>

              <section class="reference-card" aria-labelledby="stagesCardTitle">
                <div class="reference-card-head">
                  <h3 class="reference-card-title" id="stagesCardTitle">Стадии сделок</h3>
                  <p class="reference-card-description">
                    Эти стадии будут использованы для выбора статуса распределения и статусов нагрузки.
                  </p>
                </div>
                <ul class="reference-list" id="stagesList"></ul>
              </section>

              <section class="reference-card" aria-labelledby="fieldsCardTitle">
                <div class="reference-card-head">
                  <h3 class="reference-card-title" id="fieldsCardTitle">Поля ответственного</h3>
                  <p class="reference-card-description">
                    Поля сделки, в которые приложение сможет записывать выбранного ответственного.
                  </p>
                </div>
                <ul class="reference-list" id="fieldsList"></ul>
              </section>
            </div>
          </div>
        </div>
      </section>
    </main>
  </div>

  <script>
    const sectionContent = {
      overview: {
        badge: "Раздел",
        title: "Общая информация",
        description: "Собираем общую информацию. Пока собрали только то, что её нет.",
      },
      distribution: {
        badge: "Раздел",
        title: "Распределение сделок",
        description: "",
      },
      stats: {
        badge: "Раздел",
        title: "Статистика",
        description: "График посещений: Рисуем прямую линию по нулям карандашом на мониторе.",
      },
      settings: {
        badge: "Раздел",
        title: "Настройки",
        description: "Конфигурация пространства-времени. Пока настроено только пространство. Времени на настройку не хватило.",
      },
    };

    const defaultPanel = document.getElementById("defaultPanel");
    const distributionPanel = document.getElementById("distributionPanel");
    const sectionBadge = document.getElementById("sectionBadge");
    const sectionTitle = document.getElementById("sectionTitle");
    const sectionDescription = document.getElementById("sectionDescription");
    const mainCard = document.querySelector(".canvas > .canvas-card");
    const menuButtons = document.querySelectorAll("[data-view]");
    const distributionStatus = document.getElementById("distributionStatus");
    const distributionReferenceGrid = document.getElementById("distributionReferenceGrid");
    const usersList = document.getElementById("usersList");
    const stagesList = document.getElementById("stagesList");
    const fieldsList = document.getElementById("fieldsList");
    const initialDistributionMemberId = __INITIAL_MEMBER_ID__;
    const distributionState = {
      isLoaded: false,
      isLoading: false,
      memberId: initialDistributionMemberId || new URLSearchParams(window.location.search).get("member_id") || "",
      auth: null,
    };

    function setDistributionStatus(message, tone) {
      distributionStatus.textContent = message;
      distributionStatus.classList.remove("is-error", "is-success");
      if (tone) {
        distributionStatus.classList.add(tone);
      }
    }

    function renderReferenceList(target, items, renderItem) {
      target.innerHTML = "";

      if (!Array.isArray(items) || items.length === 0) {
        const emptyItem = document.createElement("li");
        emptyItem.className = "reference-item";

        const title = document.createElement("div");
        title.className = "reference-item-title";
        title.textContent = "Справочник пока пуст";
        emptyItem.appendChild(title);

        target.appendChild(emptyItem);
        return;
      }

      items.forEach((item) => {
        target.appendChild(renderItem(item));
      });
    }

    function buildReferenceItem(titleText, metaText, pills) {
      const item = document.createElement("li");
      item.className = "reference-item";

      const title = document.createElement("div");
      title.className = "reference-item-title";
      title.textContent = titleText;
      item.appendChild(title);

      if (Array.isArray(pills) && pills.length > 0) {
        const pillRow = document.createElement("div");
        pillRow.className = "reference-pill-row";

        pills.forEach((pillConfig) => {
          const pill = document.createElement("span");
          pill.className = `reference-pill${pillConfig.muted ? " is-muted" : ""}`;
          pill.textContent = pillConfig.label;
          pillRow.appendChild(pill);
        });

        item.appendChild(pillRow);
      }

      if (metaText) {
        const meta = document.createElement("div");
        meta.className = "reference-item-meta";
        meta.textContent = metaText;
        item.appendChild(meta);
      }

      return item;
    }

    function renderDistributionReferenceData(payload) {
      renderReferenceList(usersList, payload.users, (user) =>
        buildReferenceItem(
          user.name || `Пользователь ${user.id || ""}`.trim(),
          `ID: ${user.id || "не указан"}`,
          [{ label: user.is_active ? "Активен" : "Неактивен", muted: !user.is_active }]
        )
      );

      renderReferenceList(stagesList, payload.stages, (stage) =>
        buildReferenceItem(
          stage.name || stage.id || "Стадия без названия",
          `ID: ${stage.id || "не указан"} · sort: ${stage.sort ?? "?"}`
        )
      );

      renderReferenceList(fieldsList, payload.responsible_fields, (field) =>
        buildReferenceItem(
          field.name || field.id || "Поле без названия",
          `Код поля: ${field.id || "не указан"}`,
          field.is_default ? [{ label: "Поле по умолчанию" }] : []
        )
      );

      distributionReferenceGrid.hidden = false;
      setDistributionStatus(
        `Справочники загружены: сотрудников ${payload.users.length}, стадий ${payload.stages.length}, полей ${payload.responsible_fields.length}.`,
        "is-success"
      );
    }

    function getBitrixAuth() {
      return new Promise((resolve) => {
        if (!window.BX24 || typeof window.BX24.init !== "function" || typeof window.BX24.getAuth !== "function") {
          resolve(null);
          return;
        }

        try {
          window.BX24.init(() => {
            const auth = window.BX24.getAuth();
            resolve(auth || null);
          });
        } catch (error) {
          console.error("BX24 auth initialization failed", error);
          resolve(null);
        }
      });
    }

    async function resolveBitrixAuth() {
      if (distributionState.auth) {
        return distributionState.auth;
      }

      const auth = await getBitrixAuth();
      distributionState.auth = auth;
      if (!distributionState.memberId && auth && auth.member_id) {
        distributionState.memberId = auth.member_id;
      }
      return distributionState.auth;
    }

    async function resolveDistributionMemberId() {
      if (distributionState.memberId) {
        return distributionState.memberId;
      }

      const auth = await resolveBitrixAuth();
      const bitrixMemberId = (auth && auth.member_id) || "";
      distributionState.memberId = bitrixMemberId;
      return distributionState.memberId;
    }

    async function syncPortalContextFromBitrix() {
      const auth = await resolveBitrixAuth();
      if (!auth || !auth.member_id || !auth.domain || !auth.access_token) {
        return;
      }

      const payload = {
        member_id: auth.member_id,
        DOMAIN: auth.domain,
        AUTH_ID: auth.access_token,
        REFRESH_ID: auth.refresh_token || "",
        PROTOCOL: "1",
      };

      const response = await fetch("/api/ui/groups/portal-context", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorPayload = await response.json().catch(() => ({}));
        throw new Error(errorPayload.detail || "Не удалось синхронизировать портал с backend.");
      }
    }

    async function loadDistributionReferenceData() {
      if (distributionState.isLoaded || distributionState.isLoading) {
        return;
      }

      const distributionMemberId = await resolveDistributionMemberId();
      if (!distributionMemberId) {
        distributionReferenceGrid.hidden = true;
        setDistributionStatus(
          "Не удалось определить member_id портала. Откройте приложение внутри Bitrix24 или завершите повторную установку, чтобы загрузить реальные справочники.",
          "is-error"
        );
        return;
      }

      distributionState.isLoading = true;
      distributionReferenceGrid.hidden = true;
      setDistributionStatus("Загружаем сотрудников, стадии сделок и поля ответственного из Bitrix24...");

      try {
        await syncPortalContextFromBitrix();
        const response = await fetch(`/api/ui/groups/reference-data?member_id=${encodeURIComponent(distributionMemberId)}`);
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "Не удалось получить справочники Bitrix24.");
        }

        renderDistributionReferenceData(payload);
        distributionState.isLoaded = true;
      } catch (error) {
        distributionReferenceGrid.hidden = true;
        setDistributionStatus(error.message || "Не удалось загрузить справочники Bitrix24.", "is-error");
      } finally {
        distributionState.isLoading = false;
      }
    }

    function setActiveView(view) {
      const content = sectionContent[view] || sectionContent.overview;
      const isDistribution = view === "distribution";

      defaultPanel.hidden = isDistribution;
      distributionPanel.hidden = !isDistribution;
      mainCard.classList.remove("is-centered");

      if (!isDistribution) {
        sectionBadge.textContent = content.badge;
        sectionTitle.textContent = content.title;
        sectionDescription.textContent = content.description;
      } else {
        loadDistributionReferenceData();
      }

      menuButtons.forEach((button) => {
        button.classList.toggle("is-active", button.dataset.view === view);
      });
    }

    menuButtons.forEach((button) => {
      button.addEventListener("click", () => setActiveView(button.dataset.view));
    });
  </script>
</body>
</html>
"""
    return template.replace("__INITIAL_MEMBER_ID__", initial_member_id_json)


def render_install_page(*, initial_member_id: str | None = None) -> str:
    initial_member_id_json = json.dumps(initial_member_id or "", ensure_ascii=False)
    template = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bitrix App Install</title>
  <script src="//api.bitrix24.tech/api/v1/"></script>
  <style>
    html, body {
      margin: 0;
      min-height: 100%;
      background: #ffffff;
    }
  </style>
</head>
<body>
  <script>
    const initialMemberId = __INITIAL_MEMBER_ID__;
    const canvasUrl = initialMemberId ? `/ui/groups?member_id=${encodeURIComponent(initialMemberId)}` : "/ui/groups";

    if (window.BX24 && typeof window.BX24.installFinish === "function") {
      window.BX24.installFinish();
    }

    window.setTimeout(function () {
      window.location.replace(canvasUrl);
    }, 300);
  </script>
</body>
</html>
"""
    return template.replace("__INITIAL_MEMBER_ID__", initial_member_id_json)
