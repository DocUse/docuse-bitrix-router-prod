GROUPS_PAGE_SCRIPT_STATE = """    const sectionContent = {
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
        description: "Технический журнал событий и назначений по сделкам.",
      },
      settings: {
        badge: "Раздел",
        title: "Настройки",
        description: "Конфигурация пространства-времени. Пока настроено только пространство. Времени на настройку не хватило.",
      },
    };

    const defaultPanel = document.getElementById("defaultPanel");
    const statsPanel = document.getElementById("statsPanel");
    const distributionPanel = document.getElementById("distributionPanel");
    const sectionBadge = document.getElementById("sectionBadge");
    const sectionTitle = document.getElementById("sectionTitle");
    const sectionDescription = document.getElementById("sectionDescription");
    const mainCard = document.querySelector(".canvas > .canvas-card");
    const menuButtons = document.querySelectorAll("[data-view]");
    const distributionStatus = document.getElementById("distributionStatus");
    const distributionGroupsPanel = document.getElementById("distributionGroupsPanel");
    const distributionGroupsList = document.getElementById("distributionGroupsList");
    const createDistributionGroupButton = document.getElementById("createDistributionGroupButton");
    const distributionForm = document.getElementById("distributionForm");
    const groupNameInput = document.getElementById("groupNameInput");
    const distributionTypeSelect = document.getElementById("distributionTypeSelect");
    const eventTypeSelect = document.getElementById("eventTypeSelect");
    const distributionStageSelect = document.getElementById("distributionStageSelect");
    const responsibleFieldSelect = document.getElementById("responsibleFieldSelect");
    const waitSecondsInput = document.getElementById("waitSecondsInput");
    const retryIntervalInput = document.getElementById("retryIntervalInput");
    const groupActiveInput = document.getElementById("groupActiveInput");
    const bulkLimitScopeSelect = document.getElementById("bulkLimitScopeSelect");
    const bulkLimitValueInput = document.getElementById("bulkLimitValueInput");
    const applyBulkLimitButton = document.getElementById("applyBulkLimitButton");
    const participantsList = document.getElementById("participantsList");
    const loadStagesList = document.getElementById("loadStagesList");
    const saveDistributionButton = document.getElementById("saveDistributionButton");
    const statsStatus = document.getElementById("statsStatus");
    const statsSummaryList = document.getElementById("statsSummaryList");
    const statsDistributionTableBody = document.getElementById("statsDistributionTableBody");
    const statsDistributionTableFoot = document.getElementById("statsDistributionTableFoot");
    const statsMembersList = document.getElementById("statsMembersList");
    const statsMembersCount = document.getElementById("statsMembersCount");
    const statsDiagnosticsList = document.getElementById("statsDiagnosticsList");
    const statsDiagnosticsCount = document.getElementById("statsDiagnosticsCount");
    const statsJournalList = document.getElementById("statsJournalList");
    const runDeliveryCheckButton = document.getElementById("runDeliveryCheckButton");
    const refreshStatsButton = document.getElementById("refreshStatsButton");
    const initialDistributionMemberId = __INITIAL_MEMBER_ID__;
    const distributionState = {
      isLoaded: false,
      isLoading: false,
      isConfigLoaded: false,
      isConfigLoading: false,
      memberId: initialDistributionMemberId || new URLSearchParams(window.location.search).get("member_id") || "",
      auth: null,
      referenceData: null,
      config: null,
      openFormRequested: false,
      formMode: "create",
      portalContextSyncKey: "",
      portalContextSynced: false,
      isCardActionLoading: false,
    };
    const statsState = {
      isLoaded: false,
      isLoading: false,
      data: null,
    };
"""
