GROUPS_PAGE_SCRIPT_ACTIONS = """    async function persistDistributionConfig(payload) {
      const distributionMemberId = await resolveDistributionMemberId();
      if (!distributionMemberId) {
        throw new Error("Не удалось определить member_id портала для сохранения.");
      }
      const response = await fetchJson(
        `/api/ui/groups/config?member_id=${encodeURIComponent(distributionMemberId)}`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
        },
      );
      distributionState.config = normalizeLoadedDistributionConfig(response.config, distributionState.referenceData || { users: [], stages: [], responsible_fields: [] });
      distributionState.isConfigLoaded = true;
      statsState.isLoaded = false;
      return response;
    }

    async function saveDistributionConfig() {
      setDistributionStatus("Сохраняем конфигурацию группы распределения...");
      try {
        await persistDistributionConfig(collectDistributionConfigFromForm());
        distributionState.openFormRequested = false;
        distributionState.formMode = "edit";
        showDistributionLanding();
        renderDistributionGroupsPanel();
      } catch (error) {
        setDistributionStatus(error.message || "Не удалось сохранить конфигурацию группы.", "is-error");
      }
    }

    async function handleToggleDistributionGroupClick() {
      if (!distributionState.config || distributionState.isCardActionLoading) {
        return;
      }

      distributionState.isCardActionLoading = true;
      clearDistributionStatus();
      renderDistributionGroupsPanel();
      try {
        await persistDistributionConfig({
          ...distributionState.config,
          is_active: !distributionState.config.is_active,
        });
        renderDistributionGroupsPanel();
      } catch (error) {
        renderDistributionGroupsPanel();
        setDistributionStatus(error.message || "Не удалось изменить активность группы.", "is-error");
      } finally {
        distributionState.isCardActionLoading = false;
        renderDistributionGroupsPanel();
      }
    }

    async function handleDeleteDistributionGroupClick() {
      if (!distributionState.config || distributionState.isCardActionLoading) {
        return;
      }
      if (!window.confirm("Удалить группу распределения?")) {
        return;
      }

      const distributionMemberId = await resolveDistributionMemberId();
      if (!distributionMemberId) {
        setDistributionStatus("Не удалось определить member_id портала для удаления.", "is-error");
        return;
      }

      distributionState.isCardActionLoading = true;
      clearDistributionStatus();
      renderDistributionGroupsPanel();
      try {
        await fetchJson(`/api/ui/groups/config?member_id=${encodeURIComponent(distributionMemberId)}`, {
          method: "DELETE",
        });
        distributionState.config = null;
        distributionState.isConfigLoaded = true;
        distributionState.openFormRequested = false;
        distributionState.formMode = "create";
        distributionState.isLoaded = false;
        showDistributionLanding();
        renderDistributionGroupsPanel();
      } catch (error) {
        setDistributionStatus(error.message || "Не удалось удалить группу.", "is-error");
      } finally {
        distributionState.isCardActionLoading = false;
        renderDistributionGroupsPanel();
      }
    }

    async function runEventDeliveryCheck() {
      const distributionMemberId = await resolveDistributionMemberId();
      if (!distributionMemberId) {
        setStatsStatus("Не удалось определить member_id портала для запуска self-test.", "is-error");
        return;
      }

      setStatsStatus("Запускаем self-test доставки события из Bitrix...");
      try {
        const payload = await fetchJson(
          `/api/ui/stats/event-delivery-check?member_id=${encodeURIComponent(distributionMemberId)}`,
          { method: "POST" },
        );
        statsState.isLoaded = false;
        setStatsStatus(
          `Self-test отправлен. check_id: ${payload.check_id}. Через пару секунд журнал обновится автоматически.`,
          "is-success",
        );
        window.setTimeout(() => {
          loadStatsData(true);
        }, 2500);
      } catch (error) {
        setStatsStatus(error.message || "Не удалось запустить self-test доставки события.", "is-error");
      }
    }

    function handleCreateDistributionGroupClick() {
      distributionState.openFormRequested = true;
      distributionState.formMode = "create";
      loadDistributionReferenceData(false);
    }

    function handleEditDistributionGroupClick() {
      distributionState.openFormRequested = true;
      distributionState.formMode = "edit";
      loadDistributionReferenceData(false);
    }

    applyBulkLimitButton.addEventListener("click", applyBulkLimitFromForm);
    createDistributionGroupButton.addEventListener("click", handleCreateDistributionGroupClick);
    saveDistributionButton.addEventListener("click", saveDistributionConfig);
    runDeliveryCheckButton.addEventListener("click", runEventDeliveryCheck);
    refreshStatsButton.addEventListener("click", () => loadStatsData(true));

    menuButtons.forEach((button) => {
      button.addEventListener("click", () => setActiveView(button.dataset.view));
    });
"""
